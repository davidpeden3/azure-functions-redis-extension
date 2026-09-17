using FakeItEasy;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Unit
{
    public class RedisPollingTriggerBaseListenerTests
    {
        [Fact]
        public async Task Loop_ContinuesPolling_AfterPollAsyncThrows()
        {
            using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            int polls = 0;

            CallbackPollingListener listener = new CallbackPollingListener(Guid.NewGuid().ToString(), _ =>
            {
                polls++;
                if (polls == 1)
                {
                    // Simulate a transient failure (e.g. a dropped connection or command timeout) on the first poll.
                    throw new RedisException("transient failure");
                }

                if (polls >= 3)
                {
                    cancellationTokenSource.Cancel();
                }

                return Task.CompletedTask;
            });

            await listener.Loop(cancellationTokenSource.Token);

            Assert.True(polls >= 3, $"Expected the loop to keep polling after an exception. It polled {polls} time(s).");
        }

        [Fact]
        public async Task StartAsync_Throws_WhenMultiplexerIsNotConnected()
        {
            CallbackPollingListener listener = new CallbackPollingListener(Guid.NewGuid().ToString(), _ => Task.CompletedTask);
            CacheMultiplexer(listener.connection, connected: false);

            await Assert.ThrowsAsync<RedisConnectionException>(() => listener.StartAsync(CancellationToken.None));
        }

        [Fact]
        public async Task StartAsync_Throws_WhenBeforePollingFails()
        {
            CallbackPollingListener listener = new CallbackPollingListener(Guid.NewGuid().ToString(), _ => Task.CompletedTask)
            {
                onBeforePolling = () => throw new RedisTimeoutException("timed out creating the consumer group", CommandStatus.Unknown)
            };
            CacheMultiplexer(listener.connection, connected: true);

            await Assert.ThrowsAsync<RedisTimeoutException>(() => listener.StartAsync(CancellationToken.None));
            Assert.Null(listener.stopTokenSource);
        }

        [Fact]
        public async Task StartAsync_ReadsServerVersion_ThenStartsPolling()
        {
            using SemaphoreSlim polled = new SemaphoreSlim(0);
            CallbackPollingListener listener = new CallbackPollingListener(Guid.NewGuid().ToString(), _ =>
            {
                polled.Release();
                return Task.CompletedTask;
            });
            CacheMultiplexer(listener.connection, connected: true, version: new Version("7.2.0"));

            await listener.StartAsync(CancellationToken.None);
            bool polledInTime = await polled.WaitAsync(TimeSpan.FromSeconds(5));

            Assert.Equal(new Version("7.2.0"), listener.serverVersion);
            Assert.True(polledInTime, "Expected the polling loop to start after StartAsync.");
        }

        [Fact]
        public async Task StopAsync_StopsThePollingLoop()
        {
            CallbackPollingListener listener = new CallbackPollingListener(Guid.NewGuid().ToString(), _ => Task.CompletedTask);
            CacheMultiplexer(listener.connection, connected: true);
            await listener.StartAsync(CancellationToken.None);

            await listener.StopAsync(CancellationToken.None);

            Assert.True(listener.stopTokenSource.IsCancellationRequested);
        }

        private static void CacheMultiplexer(string connection, bool connected, Version version = null)
        {
            IServer server = A.Fake<IServer>();
            A.CallTo(() => server.Version).Returns(version ?? new Version("6.2.0"));
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.IsConnected).Returns(connected);
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { server });
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(connection, new Lazy<Task<IConnectionMultiplexer>>(() => Task.FromResult(multiplexer)));
        }

        private sealed class CallbackPollingListener : RedisPollingTriggerBaseListener
        {
            private readonly Func<CancellationToken, Task> onPoll;
            internal Func<Task> onBeforePolling = () => Task.CompletedTask;

            public CallbackPollingListener(string connection, Func<CancellationToken, Task> onPoll)
                : base("name", null, null, connection, "key", TimeSpan.FromMilliseconds(1), 1, false, null, A.Fake<ILogger>())
            {
                this.onPoll = onPoll;
            }

            public override Task BeforePollingAsync()
            {
                return onBeforePolling();
            }

            public override Task PollAsync(CancellationToken cancellationToken)
            {
                return onPoll(cancellationToken);
            }
        }
    }
}
