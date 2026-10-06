using FakeItEasy;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Unit
{
    public class RedisStreamListenerTests
    {
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
        public async Task StopAsync_DeletesTheConsumerFromTheGroup()
        {
            IDatabase database = A.Fake<IDatabase>();
            A.CallTo(database).WithReturnType<Task<bool>>().Where(call => call.Method.Name == nameof(IDatabase.StreamCreateConsumerGroupAsync))
                .Returns(true);
            A.CallTo(database).WithReturnType<Task<StreamEntry[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamReadGroupAsync))
                .Returns(Array.Empty<StreamEntry>());
            RedisStreamListener listener = CreateListener(database);
            await listener.StartAsync(CancellationToken.None);

            await listener.StopAsync(CancellationToken.None);

            A.CallTo(() => database.StreamDeleteConsumerAsync("key", "name", listener.consumerName, A<CommandFlags>._)).MustHaveHappenedOnceExactly();
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

        private static IDatabase SeedEntries(params StreamEntry[] entries)
        {
            IDatabase database = A.Fake<IDatabase>();
            A.CallTo(database).WithReturnType<Task<StreamEntry[]>>().Where(call => call.Method.Name == nameof(IDatabase.StreamReadGroupAsync))
                .Returns(entries);
            return database;
        }

        private static RedisStreamListener CreatePollingListener(IDatabase database, bool batch, FunctionResult result)
        {
            ITriggeredFunctionExecutor executor = A.Fake<ITriggeredFunctionExecutor>();
            A.CallTo(() => executor.TryExecuteAsync(A<TriggeredFunctionData>._, A<CancellationToken>._)).Returns(result);
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.GetDatabase(A<int>._, A<object>._)).Returns(database);
            RedisStreamListener listener = new RedisStreamListener("name", null, null, Guid.NewGuid().ToString(), "key", TimeSpan.FromMilliseconds(1), 2, batch, executor, A.Fake<ILogger>());
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
            return new RedisStreamListener("name", null, null, connection, "key", TimeSpan.FromMilliseconds(1), 1, false, A.Fake<ITriggeredFunctionExecutor>(), A.Fake<ILogger>());
        }
    }
}
