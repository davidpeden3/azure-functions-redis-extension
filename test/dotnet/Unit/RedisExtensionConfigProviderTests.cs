using FakeItEasy;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Unit
{
    public class RedisExtensionConfigProviderTests
    {
        [Fact]
        public async Task GetOrCreateConnectionMultiplexerAsync_ConcurrentCallers_ShareOneConnect()
        {
            string connection = Guid.NewGuid().ToString();
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            int connects = 0;
            using SemaphoreSlim connectStarted = new SemaphoreSlim(0);
            using SemaphoreSlim connectReleased = new SemaphoreSlim(0);

            Task<IConnectionMultiplexer>[] callers = Enumerable.Range(0, 96).Select(_ => Task.Run(() =>
                RedisExtensionConfigProvider.GetOrCreateConnectionMultiplexerAsync(connection, async () =>
                {
                    Interlocked.Increment(ref connects);
                    connectStarted.Release();
                    await connectReleased.WaitAsync();
                    return multiplexer;
                }))).ToArray();

            // Hold the first connect open until every caller has raced in, then let it finish.
            await connectStarted.WaitAsync();
            connectReleased.Release();
            IConnectionMultiplexer[] results = await Task.WhenAll(callers);

            Assert.Equal(1, connects);
            Assert.All(results, result => Assert.Same(multiplexer, result));
        }

        [Fact]
        public async Task GetOrCreateConnectionMultiplexerAsync_FailedConnect_IsNotCached()
        {
            string connection = Guid.NewGuid().ToString();
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            int connects = 0;

            await Assert.ThrowsAsync<RedisConnectionException>(() => RedisExtensionConfigProvider.GetOrCreateConnectionMultiplexerAsync(connection, () =>
            {
                connects++;
                throw new RedisConnectionException(ConnectionFailureType.UnableToConnect, "transient failure");
            }));

            IConnectionMultiplexer result = await RedisExtensionConfigProvider.GetOrCreateConnectionMultiplexerAsync(connection, () =>
            {
                connects++;
                return Task.FromResult(multiplexer);
            });

            Assert.Equal(2, connects);
            Assert.Same(multiplexer, result);
        }

        [Fact]
        public async Task GetOrCreateConnectionMultiplexerAsync_SuccessfulConnect_IsReused()
        {
            string connection = Guid.NewGuid().ToString();
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            int connects = 0;
            Func<Task<IConnectionMultiplexer>> factory = () =>
            {
                connects++;
                return Task.FromResult(multiplexer);
            };

            IConnectionMultiplexer first = await RedisExtensionConfigProvider.GetOrCreateConnectionMultiplexerAsync(connection, factory);
            IConnectionMultiplexer second = await RedisExtensionConfigProvider.GetOrCreateConnectionMultiplexerAsync(connection, factory);

            Assert.Equal(1, connects);
            Assert.Same(first, second);
        }
    }
}
