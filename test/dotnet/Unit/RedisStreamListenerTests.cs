using FakeItEasy;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Unit
{
    public class RedisStreamListenerTests
    {
        private const int ClaimMinIdleTimeInMs = 100;
        private const int MaxDeliveryCount = 3;

        [Fact]
        public async Task StartAsync_Throws_WhenConsumerGroupCreationThrows()
        {
            IDatabase database = A.Fake<IDatabase>();
            A.CallTo(database).WithReturnType<Task<bool>>().Where(call => call.Method.Name == nameof(IDatabase.StreamCreateConsumerGroupAsync))
                .Throws(new RedisTimeoutException("timed out creating the consumer group", CommandStatus.Unknown));
            RedisStreamListener listener = CreateListener(database);

            await Assert.ThrowsAsync<RedisTimeoutException>(() => listener.StartAsync(CancellationToken.None));
        }

        [Fact]
        public async Task StartAsync_Succeeds_WhenTheConsumerGroupAlreadyExists()
        {
            IDatabase database = A.Fake<IDatabase>();
            A.CallTo(database).WithReturnType<Task<bool>>().Where(call => call.Method.Name == nameof(IDatabase.StreamCreateConsumerGroupAsync))
                .Throws(new RedisServerException("BUSYGROUP Consumer Group name already exists"));
            A.CallTo(database).WithReturnType<Task<StreamEntry[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamReadGroupAsync))
                .Returns(Array.Empty<StreamEntry>());
            RedisStreamListener listener = CreateListener(database);

            await listener.StartAsync(CancellationToken.None);
        }

        [Fact]
        public async Task StopAsync_DeletesTheConsumerOnlyWhenItHasNoPendingEntries()
        {
            IDatabase database = A.Fake<IDatabase>();
            A.CallTo(database).WithReturnType<Task<bool>>().Where(call => call.Method.Name == nameof(IDatabase.StreamCreateConsumerGroupAsync))
                .Returns(true);
            A.CallTo(database).WithReturnType<Task<StreamEntry[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamReadGroupAsync))
                .Returns(Array.Empty<StreamEntry>());
            A.CallTo(() => database.ScriptEvaluateAsync(A<string>._, A<RedisKey[]>._, A<RedisValue[]>._, A<CommandFlags>._))
                .Returns(RedisResult.Create(1));
            RedisStreamListener listener = CreateListener(database);
            await listener.StartAsync(CancellationToken.None);

            await listener.StopAsync(CancellationToken.None);

            A.CallTo(() => database.ScriptEvaluateAsync(
                    RedisStreamListener.DeleteConsumerWithoutPendingEntriesScript,
                    A<RedisKey[]>.That.IsSameSequenceAs(new RedisKey[] { "key" }),
                    A<RedisValue[]>.That.IsSameSequenceAs(new RedisValue[] { "name", listener.consumerName }),
                    A<CommandFlags>._))
                .MustHaveHappenedOnceExactly();
            A.CallTo(() => database.StreamDeleteConsumerAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue>._, A<CommandFlags>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task StopAsync_DoesNothing_WhenTheListenerNeverStarted()
        {
            IDatabase database = A.Fake<IDatabase>();
            RedisStreamListener listener = CreateListener(database);

            await listener.StopAsync(CancellationToken.None);

            A.CallTo(database).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_AcknowledgesTheEntry_WhenTheFunctionSucceeds()
        {
            StreamEntry entry = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries(entry);
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true));

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => database.StreamAcknowledgeAsync("key", "name", entry.Id, A<CommandFlags>._)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task PollAsync_LeavesTheEntryPending_WhenTheFunctionFails()
        {
            StreamEntry entry = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries(entry);
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(new InvalidOperationException("function failed")));

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => database.StreamAcknowledgeAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue>._, A<CommandFlags>._)).MustNotHaveHappened();
            A.CallTo(() => database.StreamAcknowledgeAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue[]>._, A<CommandFlags>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_AcknowledgesTheBatch_WhenTheBatchFunctionSucceeds()
        {
            StreamEntry first = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            StreamEntry second = new StreamEntry("2-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries(first, second);
            RedisStreamListener listener = CreatePollingListener(database, batch: true, new FunctionResult(true));

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => database.StreamAcknowledgeAsync("key", "name", A<RedisValue[]>.That.IsSameSequenceAs(new[] { first.Id, second.Id }), A<CommandFlags>._)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task PollAsync_LeavesEveryEntryInTheBatchPending_WhenTheBatchFunctionFails()
        {
            StreamEntry first = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            StreamEntry second = new StreamEntry("2-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries(first, second);
            RedisStreamListener listener = CreatePollingListener(database, batch: true, new FunctionResult(new InvalidOperationException("function failed")));

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => database.StreamAcknowledgeAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue>._, A<CommandFlags>._)).MustNotHaveHappened();
            A.CallTo(() => database.StreamAcknowledgeAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue[]>._, A<CommandFlags>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_NeverReadsPendingEntries_WhenReclaimingIsOff()
        {
            IDatabase database = SeedEntries();
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true), new RedisOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamPendingMessagesAsync)).MustNotHaveHappened();
            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamClaimAsync)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_ClaimsAndRunsAnIdlePendingEntry_UnderTheDeliveryLimit()
        {
            StreamEntry entry = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries();
            SeedPendingEntries(database, PendingEntry(entry.Id, idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: MaxDeliveryCount - 1));
            SeedClaimedEntries(database, entry);
            ITriggeredFunctionExecutor executor = A.Fake<ITriggeredFunctionExecutor>();
            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>._, A<CancellationToken>._)).Returns(new FunctionResult(true));
            RedisStreamListener listener = CreatePollingListener(database, batch: false, executor, ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => database.StreamClaimAsync("key", "name", listener.consumerName, ClaimMinIdleTimeInMs, A<RedisValue[]>.That.IsSameSequenceAs(new[] { entry.Id }), A<CommandFlags>._)).MustHaveHappenedOnceExactly();
            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>.That.Matches(data => ((StreamEntry)data.TriggerValue).Id == entry.Id), A<CancellationToken>._)).MustHaveHappenedOnceExactly();
            A.CallTo(() => database.StreamAcknowledgeAsync("key", "name", entry.Id, A<CommandFlags>._)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task PollAsync_LeavesAClaimedEntryPending_WhenItsFunctionFails()
        {
            StreamEntry entry = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries();
            SeedPendingEntries(database, PendingEntry(entry.Id, idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: 1));
            SeedClaimedEntries(database, entry);
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(new InvalidOperationException("function failed")), ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamClaimAsync)).MustHaveHappenedOnceExactly();
            A.CallTo(() => database.StreamAcknowledgeAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue>._, A<CommandFlags>._)).MustNotHaveHappened();
            A.CallTo(() => database.StreamAcknowledgeAsync(A<RedisKey>._, A<RedisValue>._, A<RedisValue[]>._, A<CommandFlags>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_RunsClaimedEntriesAsOneBatch_WhenTheFunctionTakesABatch()
        {
            StreamEntry first = new StreamEntry("1-0", Array.Empty<NameValueEntry>());
            StreamEntry second = new StreamEntry("2-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries();
            SeedPendingEntries(database, PendingEntry(first.Id, idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: 1), PendingEntry(second.Id, idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: 1));
            SeedClaimedEntries(database, first, second);
            ITriggeredFunctionExecutor executor = A.Fake<ITriggeredFunctionExecutor>();
            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>._, A<CancellationToken>._)).Returns(new FunctionResult(true));
            RedisStreamListener listener = CreatePollingListener(database, batch: true, executor, ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>.That.Matches(data => ((StreamEntry[])data.TriggerValue).Length == 2), A<CancellationToken>._)).MustHaveHappenedOnceExactly();
            A.CallTo(() => database.StreamAcknowledgeAsync("key", "name", A<RedisValue[]>.That.IsSameSequenceAs(new[] { first.Id, second.Id }), A<CommandFlags>._)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task PollAsync_LeavesAnEntryPending_BelowTheIdleThreshold()
        {
            IDatabase database = SeedEntries();
            SeedPendingEntries(database, PendingEntry("1-0", idleTimeInMs: ClaimMinIdleTimeInMs - 1, deliveryCount: 1));
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true), ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamClaimAsync)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_LeavesAnEntryPending_AtTheDeliveryLimit()
        {
            IDatabase database = SeedEntries();
            SeedPendingEntries(database, PendingEntry("1-0", idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: MaxDeliveryCount));
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true), ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamClaimAsync)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_ReadsPastAPageOfEntriesAtTheDeliveryLimit_ToReachAClaimableOne()
        {
            StreamEntry claimable = new StreamEntry("3-0", Array.Empty<NameValueEntry>());
            IDatabase database = SeedEntries();
            // The listener's batch size is 2. The first page is full of entries at the limit. Read alone it would hide the claimable entry behind them.
            A.CallTo(() => database.StreamPendingMessagesAsync("key", "name", 2, RedisValue.Null, StreamPosition.Beginning, A<RedisValue?>._, A<CommandFlags>._))
                .Returns(new[] { PendingEntry("1-0", idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: MaxDeliveryCount), PendingEntry("2-5", idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: MaxDeliveryCount) });
            A.CallTo(() => database.StreamPendingMessagesAsync("key", "name", 2, RedisValue.Null, (RedisValue)"2-6", A<RedisValue?>._, A<CommandFlags>._))
                .Returns(new[] { PendingEntry(claimable.Id, idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: 1) });
            SeedClaimedEntries(database, claimable);
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true), ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => database.StreamClaimAsync("key", "name", listener.consumerName, ClaimMinIdleTimeInMs, A<RedisValue[]>.That.IsSameSequenceAs(new[] { claimable.Id }), A<CommandFlags>._)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task PollAsync_SkipsAClaimedIdWithoutAnEntry()
        {
            IDatabase database = SeedEntries();
            SeedPendingEntries(database, PendingEntry("1-0", idleTimeInMs: ClaimMinIdleTimeInMs, deliveryCount: 1));
            SeedClaimedEntries(database, StreamEntry.Null);
            ITriggeredFunctionExecutor executor = A.Fake<ITriggeredFunctionExecutor>();
            RedisStreamListener listener = CreatePollingListener(database, batch: false, executor, ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);

            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>._, A<CancellationToken>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task PollAsync_ReadsPendingEntriesOnce_UntilTheClaimIntervalHasPassed()
        {
            IDatabase database = SeedEntries();
            SeedPendingEntries(database);
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true), ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);
            await listener.PollAsync(CancellationToken.None);

            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamPendingMessagesAsync)).MustHaveHappenedOnceExactly();
        }

        [Fact]
        public async Task PollAsync_ReadsPendingEntriesAgain_OnceTheClaimIntervalHasPassed()
        {
            IDatabase database = SeedEntries();
            SeedPendingEntries(database);
            RedisStreamListener listener = CreatePollingListener(database, batch: false, new FunctionResult(true), ReclaimingOptions());

            await listener.PollAsync(CancellationToken.None);
            listener.nextClaimUtc = DateTime.UtcNow;
            await listener.PollAsync(CancellationToken.None);

            A.CallTo(database).Where(call => call.Method.Name == nameof(IDatabase.StreamPendingMessagesAsync)).MustHaveHappenedTwiceExactly();
        }

        [Theory]
        [InlineData("1700000000000-0", "1700000000000-1")]
        [InlineData("1700000000000-18446744073709551615", "1700000000001-0")]
        public void NextEntryId_IsTheSmallestIdAfterTheGivenOne(string id, string expected)
        {
            Assert.Equal(expected, (string)RedisStreamListener.NextEntryId(id));
        }

        private static RedisOptions ReclaimingOptions()
        {
            return new RedisOptions { StreamClaimMinIdleTimeInMs = ClaimMinIdleTimeInMs, StreamMaxDeliveryCount = MaxDeliveryCount };
        }

        private static IDatabase SeedEntries(params StreamEntry[] entries)
        {
            IDatabase database = A.Fake<IDatabase>();
            A.CallTo(database).WithReturnType<Task<StreamEntry[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamReadGroupAsync))
                .Returns(entries);
            return database;
        }

        private static void SeedPendingEntries(IDatabase database, params StreamPendingMessageInfo[] pending)
        {
            A.CallTo(database).WithReturnType<Task<StreamPendingMessageInfo[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamPendingMessagesAsync))
                .Returns(pending);
        }

        private static void SeedClaimedEntries(IDatabase database, params StreamEntry[] claimed)
        {
            A.CallTo(database).WithReturnType<Task<StreamEntry[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamClaimAsync))
                .Returns(claimed);
        }

        /// <summary>
        /// StackExchange.Redis builds a pending entry only from a server reply. Its constructor is internal.
        /// </summary>
        private static StreamPendingMessageInfo PendingEntry(RedisValue id, long idleTimeInMs, int deliveryCount)
        {
            ConstructorInfo constructor = typeof(StreamPendingMessageInfo).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance).Single();
            return (StreamPendingMessageInfo)constructor.Invoke(new object[] { id, (RedisValue)"consumer", idleTimeInMs, deliveryCount });
        }

        private static RedisStreamListener CreatePollingListener(IDatabase database, bool batch, FunctionResult result, RedisOptions options = null)
        {
            ITriggeredFunctionExecutor executor = A.Fake<ITriggeredFunctionExecutor>();
            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>._, A<CancellationToken>._)).Returns(result);
            return CreatePollingListener(database, batch, executor, options);
        }

        private static RedisStreamListener CreatePollingListener(IDatabase database, bool batch, ITriggeredFunctionExecutor executor, RedisOptions options = null)
        {
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.GetDatabase(A<int>._, A<object>._)).Returns(database);
            RedisStreamListener listener = new RedisStreamListener("name", null, null, Guid.NewGuid().ToString(), "key", TimeSpan.FromMilliseconds(1), 2, batch, options ?? new RedisOptions(), executor, A.Fake<ILogger>());
            listener.multiplexer = multiplexer;
            return listener;
        }

        private static RedisStreamListener CreateListener(IDatabase database)
        {
            string connection = Guid.NewGuid().ToString();
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.IsConnected).Returns(true);
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { A.Fake<IServer>() });
            A.CallTo(() => multiplexer.GetDatabase(A<int>._, A<object>._)).Returns(database);
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(connection, new Lazy<Task<IConnectionMultiplexer>>(() => Task.FromResult(multiplexer)));
            return new RedisStreamListener("name", null, null, connection, "key", TimeSpan.FromMilliseconds(1), 1, false, new RedisOptions(), A.Fake<ITriggeredFunctionExecutor>(), A.Fake<ILogger>());
        }
    }
}
