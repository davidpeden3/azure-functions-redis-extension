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
    public class RedisListListenerTests
    {
        [Fact]
        public async Task StartAsync_Throws_WhenTheMultiplexerIsNotConnected()
        {
            RedisListListener listener = CreateListener(connected: false, new Version("7.2.0"));

            await Assert.ThrowsAsync<RedisConnectionException>(() => listener.StartAsync(CancellationToken.None));

            Assert.Null(listener.serverVersion);
        }

        [Fact]
        public async Task StartAsync_ReadsTheServerVersion_WhenTheMultiplexerIsConnected()
        {
            RedisListListener listener = CreateListener(connected: true, new Version("7.2.0"));

            await listener.StartAsync(CancellationToken.None);

            Assert.Equal(new Version("7.2.0"), listener.serverVersion);
        }

        private static RedisListListener CreateListener(bool connected, Version serverVersion)
        {
            string connection = Guid.NewGuid().ToString();
            IServer server = A.Fake<IServer>();
            A.CallTo(() => server.Version).Returns(serverVersion);
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.IsConnected).Returns(connected);
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { server });
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(connection, new Lazy<Task<IConnectionMultiplexer>>(() => Task.FromResult(multiplexer)));
            return new RedisListListener("name", null, null, connection, "key", TimeSpan.FromMilliseconds(1), 1, ListDirection.LEFT, false, A.Fake<ITriggeredFunctionExecutor>(), A.Fake<ILogger>());
        }
    }
}
