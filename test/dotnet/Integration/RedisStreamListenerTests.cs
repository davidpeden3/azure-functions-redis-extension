using FakeItEasy;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    [Collection("RedisTriggerTests")]
    public class RedisStreamListenerTests
    {
        private const string Key = nameof(RedisStreamListenerTests);
        private const string Group = nameof(RedisStreamListenerTests);

        [Fact]
        public async Task StopAsync_KeepsTheConsumerAndItsPendingEntries_WhenAnEntryIsUnacknowledged()
        {
            StreamPendingMessageInfo[] pending;
            StreamConsumerInfo[] consumers;
            RedisValue id;
            string consumerName;

            using (Process redisProcess = IntegrationTestHelpers.StartRedis(IntegrationTestHelpers.Redis60))
            using (ConnectionMultiplexer multiplexer = await ConnectAsync())
            {
                IDatabase db = multiplexer.GetDatabase();
                RedisStreamListener listener = CreateListener(multiplexer);
                consumerName = listener.consumerName;
                await db.StreamCreateConsumerGroupAsync(Key, Group, StreamPosition.Beginning);
                id = await db.StreamAddAsync(Key, "name", "value");
                // Delivered to the listener's consumer and never acknowledged, as an entry still in flight is.
                await db.StreamReadGroupAsync(Key, Group, consumerName);
                await listener.StartAsync(CancellationToken.None);

                await listener.StopAsync(CancellationToken.None);

                pending = await db.StreamPendingMessagesAsync(Key, Group, 10, consumerName);
                consumers = await db.StreamConsumerInfoAsync(Key, Group);
                IntegrationTestHelpers.StopRedis(redisProcess);
            }

            Assert.Equal(id, Assert.Single(pending).MessageId);
            Assert.Equal(consumerName, (string)Assert.Single(consumers).Name);
        }

        [Fact]
        public async Task StopAsync_DeletesTheConsumer_WhenNothingIsPending()
        {
            StreamConsumerInfo[] consumers;

            using (Process redisProcess = IntegrationTestHelpers.StartRedis(IntegrationTestHelpers.Redis60))
            using (ConnectionMultiplexer multiplexer = await ConnectAsync())
            {
                IDatabase db = multiplexer.GetDatabase();
                RedisStreamListener listener = CreateListener(multiplexer);
                await db.StreamCreateConsumerGroupAsync(Key, Group, StreamPosition.Beginning);
                RedisValue id = await db.StreamAddAsync(Key, "name", "value");
                // Delivered to the listener's consumer and acknowledged. The consumer exists with nothing pending.
                await db.StreamReadGroupAsync(Key, Group, listener.consumerName);
                await db.StreamAcknowledgeAsync(Key, Group, id);
                await listener.StartAsync(CancellationToken.None);

                await listener.StopAsync(CancellationToken.None);

                consumers = await db.StreamConsumerInfoAsync(Key, Group);
                IntegrationTestHelpers.StopRedis(redisProcess);
            }

            Assert.Empty(consumers);
        }

        private static async Task<ConnectionMultiplexer> ConnectAsync()
        {
            return await ConnectionMultiplexer.ConnectAsync(await RedisUtilities.ResolveConfigurationOptionsAsync(IntegrationTestHelpers.localsettings, null, IntegrationTestHelpers.ConnectionString, "test"));
        }

        /// <summary>
        /// A listener on the test's own multiplexer. The connection name is unique to the listener so that the
        /// process-wide multiplexer cache hands it this multiplexer rather than one cached by another test.
        /// </summary>
        private static RedisStreamListener CreateListener(IConnectionMultiplexer multiplexer)
        {
            string connection = Guid.NewGuid().ToString();
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(connection, new Lazy<Task<IConnectionMultiplexer>>(() => Task.FromResult(multiplexer)));
            return new RedisStreamListener(Group, null, null, connection, Key, TimeSpan.FromMilliseconds(IntegrationTestHelpers.PollingIntervalShort), 1, false, A.Fake<ITriggeredFunctionExecutor>(), A.Fake<ILogger>());
        }
    }
}
