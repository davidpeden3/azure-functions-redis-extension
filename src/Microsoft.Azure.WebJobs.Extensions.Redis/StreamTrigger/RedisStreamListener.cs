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

        /// <summary>
        /// Executes the function for one entry and acknowledges the entry only when the function succeeds.
        /// TryExecuteAsync reports a failed function through its result rather than by throwing. A failed entry
        /// stays in the consumer group's pending entries list, where it can be claimed and retried.
        /// </summary>
        private async Task ExecuteAsync(StreamEntry value, CancellationToken cancellationToken)
        {
            IDatabase db = multiplexer.GetDatabase();
            FunctionResult result = await executor.TryExecuteAsync(new TriggeredFunctionData() { TriggerValue = value }, cancellationToken);
            if (!result.Succeeded)
            {
                logger?.LogWarning($"{logPrefix} The function failed for entry '{value.Id}'. The entry was not acknowledged and remains pending for the consumer group '{name}'.");
                return;
            }

            long acknowledged = await db.StreamAcknowledgeAsync(key, name, value.Id);
            logger?.LogDebug($"{logPrefix} Acknowledged {acknowledged} entries from the stream at key '{key}'.");
        }

        /// <summary>
        /// Executes the function for a batch of entries and acknowledges the batch only when the function succeeds.
        /// A failed batch stays in the consumer group's pending entries list as a whole.
        /// </summary>
        private async Task ExecuteBatchAsync(StreamEntry[] values, CancellationToken cancellationToken)
        {
            IDatabase db = multiplexer.GetDatabase();
            FunctionResult result = await executor.TryExecuteAsync(new TriggeredFunctionData() { TriggerValue = values }, cancellationToken);
            if (!result.Succeeded)
            {
                logger?.LogWarning($"{logPrefix} The function failed for a batch of {values.Length} entries. The entries were not acknowledged and remain pending for the consumer group '{name}'.");
                return;
            }

            long acknowledged = await db.StreamAcknowledgeAsync(key, name, Array.ConvertAll(values, value => value.Id));
            logger?.LogDebug($"{logPrefix} Acknowledged {acknowledged} entries from the stream at key '{key}'.");
        }

        public override async Task BeforeClosingAsync()
        {
            IDatabase db = multiplexer.GetDatabase();
            logger?.LogInformation($"{logPrefix} Attempting to delete consumer name '{consumerName}' from the consumer group '{name}' for the stream at key '{key}'.");
            long pending = await db.StreamDeleteConsumerAsync(key, name, consumerName);
            logger?.LogInformation($"{logPrefix} Successfully deleted consumer name '{consumerName}' from the consumer group '{name}' for the stream at key '{key}'. There were {pending} pending messages for the consumer.");
        }
    }
}
