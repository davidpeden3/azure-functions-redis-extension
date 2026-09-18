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

        private static RedisStreamListener CreateListener(IDatabase database)
        {
            string connection = Guid.NewGuid().ToString();
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { A.Fake<IServer>() });
            A.CallTo(() => multiplexer.GetDatabase(A<int>._, A<object>._)).Returns(database);
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(connection, new Lazy<Task<IConnectionMultiplexer>>(() => Task.FromResult(multiplexer)));
            return new RedisStreamListener("name", null, null, connection, "key", TimeSpan.FromMilliseconds(1), 1, false, A.Fake<ITriggeredFunctionExecutor>(), A.Fake<ILogger>());
        }
    }
}
