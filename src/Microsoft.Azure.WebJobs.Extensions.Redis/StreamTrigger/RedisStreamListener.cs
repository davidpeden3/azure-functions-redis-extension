using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
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
        internal RedisOptions options;
        internal DateTime nextClaimUtc;

        public RedisStreamListener(string name, IConfiguration configuration, AzureComponentFactory azureComponentFactory, string connection, string key, TimeSpan pollingInterval, int maxBatchSize, bool batch, RedisOptions options, ITriggeredFunctionExecutor executor, ILogger logger)
            : base(name, configuration, azureComponentFactory, connection, key, pollingInterval, maxBatchSize, batch, executor, logger)
        {
            this.options = options;
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
            StreamEntry[] claimed = await ClaimIdlePendingEntriesAsync(db);
            if (claimed.Length > 0)
            {
                await ExecuteEntriesAsync(claimed, cancellationToken);
            }

            StreamEntry[] entries = await db.StreamReadGroupAsync(key, name, consumerName, count: maxBatchSize);
            logger?.LogDebug($"{logPrefix} Received {entries.Length} entries from the stream at key '{key}'.");
            if (entries.Length == 0)
            {
                return;
            }

            await ExecuteEntriesAsync(entries, cancellationToken);
        }

        /// <summary>
        /// Claims the consumer group's idle pending entries so that an entry whose function failed or whose consumer
        /// went away with the entry in flight is run again. XREADGROUP with '>' delivers new entries only. Without
        /// this step nothing ever runs a pending entry a second time. Redis counts deliveries per entry: XREADGROUP
        /// sets the count to 1 and every XCLAIM adds 1. An entry that has reached the delivery limit stays pending for
        /// the application to deal with and is never claimed again. XCLAIM checks the idle time again on the server,
        /// so two instances that race for one entry cannot both claim it: the first claim resets the idle time and the
        /// second claims nothing. XAUTOCLAIM is not used because it cannot filter by delivery count.
        /// </summary>
        private async Task<StreamEntry[]> ClaimIdlePendingEntriesAsync(IDatabase db)
        {
            if (!options.StreamReclaimEnabled)
            {
                return Array.Empty<StreamEntry>();
            }

            // Once per idle interval per listener rather than on every poll. A host with many stream triggers would
            // otherwise double its Redis command volume for a step that rarely finds anything.
            DateTime now = DateTime.UtcNow;
            if (now < nextClaimUtc)
            {
                return Array.Empty<StreamEntry>();
            }
            nextClaimUtc = now.AddMilliseconds(options.StreamClaimMinIdleTimeInMs);

            RedisValue[] ids = await FindClaimableEntryIdsAsync(db);
            if (ids.Length == 0)
            {
                return Array.Empty<StreamEntry>();
            }

            StreamEntry[] claimed = await db.StreamClaimAsync(key, name, consumerName, options.StreamClaimMinIdleTimeInMs, ids);
            // An id with no entry behind it was trimmed from the stream after it was delivered. There is nothing to run.
            StreamEntry[] entries = claimed.Where(entry => !entry.IsNull).ToArray();
            logger?.LogInformation($"{logPrefix} Claimed {entries.Length} idle pending entries from the consumer group '{name}' for the stream at key '{key}'.");
            return entries;
        }

        /// <summary>
        /// Reads the group's pending entries without a consumer filter. Entries stranded under a consumer that no
        /// longer exists are therefore included. Keeps the ones idle for at least the claim interval and below the
        /// delivery limit. XPENDING returns entries in id order. A page that holds only entries at the delivery limit
        /// is therefore read past rather than letting those entries hide claimable ones behind them.
        /// </summary>
        private async Task<RedisValue[]> FindClaimableEntryIdsAsync(IDatabase db)
        {
            List<RedisValue> ids = new List<RedisValue>();
            RedisValue minId = StreamPosition.Beginning;
            while (ids.Count < maxBatchSize)
            {
                StreamPendingMessageInfo[] page = await db.StreamPendingMessagesAsync(key, name, maxBatchSize, RedisValue.Null, minId);
                ids.AddRange(page
                    .Where(pending => pending.IdleTimeInMilliseconds >= options.StreamClaimMinIdleTimeInMs && pending.DeliveryCount < options.StreamMaxDeliveryCount)
                    .Select(pending => pending.MessageId));
                if (page.Length < maxBatchSize)
                {
                    break;
                }
                minId = NextEntryId(page[page.Length - 1].MessageId);
            }
            return ids.Take(maxBatchSize).ToArray();
        }

        /// <summary>
        /// The smallest id that can follow the given one. A stream id is '[milliseconds]-[sequence]' and the sequence
        /// counts up within a millisecond. The next id is therefore the same millisecond with the sequence one higher.
        /// XPENDING's range is inclusive at both ends and the exclusive '(' form needs Redis 6.2.
        /// </summary>
        internal static RedisValue NextEntryId(RedisValue id)
        {
            string[] parts = ((string)id).Split('-');
            ulong milliseconds = ulong.Parse(parts[0]);
            ulong sequence = ulong.Parse(parts[1]);
            return sequence == ulong.MaxValue ? $"{milliseconds + 1}-0" : $"{milliseconds}-{sequence + 1}";
        }

        private Task ExecuteEntriesAsync(StreamEntry[] entries, CancellationToken cancellationToken)
        {
            return batch
                ? ExecuteBatchAsync(entries, cancellationToken)
                : Task.WhenAll(entries.Select(entry => ExecuteAsync(entry, cancellationToken)));
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
