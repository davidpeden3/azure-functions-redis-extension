using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Extensions.Redis
{
    /// <summary>
    /// Responsible for managing connections and listening to a given Azure Redis Cache.
    /// </summary>
    internal sealed class RedisStreamListener : RedisPollingTriggerBaseListener
    {
        // XGROUP DELCONSUMER discards the consumer's pending entries along with it. An entry that was delivered
        // but never acknowledged must stay pending so that it can still be claimed and retried. The consumer is
        // therefore deleted only when it has nothing pending. Every host process on an instance reads under the
        // same consumer name. The check and the delete run as one script so that a process still reading under
        // that name cannot have an entry delivered to it between the two.
        internal const string DeleteConsumerWithoutPendingEntriesScript = @"
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then
    return 0
end
redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
return 1";

        internal string consumerName;
        internal string entriesReadKey;

        public RedisStreamListener(string name, IConfiguration configuration, AzureComponentFactory azureComponentFactory, string connection, string key, TimeSpan pollingInterval, int maxBatchSize, bool batch, ITriggeredFunctionExecutor executor, ILogger logger)
            : base(name, configuration, azureComponentFactory, connection, key, pollingInterval, maxBatchSize, batch, executor, logger)
        {
            this.consumerName = Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID") ?? Guid.NewGuid().ToString();
            this.logPrefix = $"[Name:{name}][Trigger:{RedisUtilities.RedisStreamTrigger}][ConsumerGroup:{name}][Key:{key}][Consumer:{consumerName}]";
            this.scaleMonitor = new RedisStreamTriggerScaleMonitor(name, configuration, azureComponentFactory, connection, maxBatchSize, key);
            this.entriesReadKey = RedisScalerProvider.GetFunctionScalerId(name, RedisUtilities.RedisStreamTrigger, key);
        }

        public override async Task BeforePollingAsync()
        {
            IDatabase db = multiplexer.GetDatabase();
            try
            {
                logger?.LogInformation($"{logPrefix} Attempting to create consumer group '{name}' for the stream at key '{key}'.");
                if (!await db.StreamCreateConsumerGroupAsync(key, name, StreamPosition.Beginning))
                {
                    logger?.LogCritical($"{logPrefix} Could not create consumer group '{name}' for the stream at key '{key}'.");
                    throw new Exception($"Could not create consumer group '{name}' for the stream at key '{key}'.");
                }
                await db.KeyDeleteAsync(entriesReadKey);
                logger?.LogInformation($"{logPrefix} Successfully created consumer group '{name}' for the stream at key '{key}'.");

            }
            catch (RedisServerException e)
            {
                if (e.Message.Contains("BUSYGROUP"))
                {
                    logger?.LogInformation($"{logPrefix} Consumer group '{name}' for the stream at key '{key}' already exists.");
                }
                else
                {
                    logger?.LogCritical($"{logPrefix} Could not create consumer group '{name}' for the stream at key '{key}'.");
                    throw;
                }
            }

            logger?.LogInformation($"{logPrefix} Beginning polling loop.");
        }

        public override async Task PollAsync(CancellationToken cancellationToken)
        {
            IDatabase db = multiplexer.GetDatabase();
            StreamEntry[] entries = await db.StreamReadGroupAsync(key, name, consumerName, count: maxBatchSize);
            logger?.LogDebug($"{logPrefix} Received {entries.Length} entries from the stream at key '{key}'.");
            if (entries.Length == 0)
            {
                return;
            }

            if (batch)
            {
                await ExecuteBatchAsync(entries, cancellationToken);
            }
            else
            {
                await Task.WhenAll(entries.Select(entry => ExecuteAsync(entry, cancellationToken)));
            }
        }

        private async Task ExecuteAsync(StreamEntry value, CancellationToken cancellationToken)
        {
            IDatabase db = multiplexer.GetDatabase();
            await executor.TryExecuteAsync(new TriggeredFunctionData() { TriggerValue = value }, cancellationToken);
            long acknowledged = await db.StreamAcknowledgeAsync(key, name, value.Id);
            logger?.LogDebug($"{logPrefix} Acknowledged {acknowledged} entries from the stream at key '{key}'.");
        }

        private async Task ExecuteBatchAsync(StreamEntry[] values, CancellationToken cancellationToken)
        {
            IDatabase db = multiplexer.GetDatabase();
            await executor.TryExecuteAsync(new TriggeredFunctionData() { TriggerValue = values }, cancellationToken);
            long acknowledged = await db.StreamAcknowledgeAsync(key, name, Array.ConvertAll(values, value => value.Id));
            logger?.LogDebug($"{logPrefix} Acknowledged {acknowledged} entries from the stream at key '{key}'.");
        }

        public override async Task BeforeClosingAsync()
        {
            IDatabase db = multiplexer.GetDatabase();
            logger?.LogInformation($"{logPrefix} Attempting to delete consumer name '{consumerName}' from the consumer group '{name}' for the stream at key '{key}'.");
            bool deleted = (bool)await db.ScriptEvaluateAsync(DeleteConsumerWithoutPendingEntriesScript, new RedisKey[] { key }, new RedisValue[] { name, consumerName });
            if (deleted)
            {
                logger?.LogInformation($"{logPrefix} Successfully deleted consumer name '{consumerName}' from the consumer group '{name}' for the stream at key '{key}'.");
            }
            else
            {
                logger?.LogInformation($"{logPrefix} Kept consumer name '{consumerName}' in the consumer group '{name}' for the stream at key '{key}' because it has pending messages. They stay pending for the consumer group to claim.");
            }
        }
    }
}
