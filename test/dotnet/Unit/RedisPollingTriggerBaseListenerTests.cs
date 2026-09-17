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

            CallbackPollingListener listener = new CallbackPollingListener(_ =>
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
        public async Task Loop_Stops_WhenTheTokenIsCancelled()
        {
            using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            int polls = 0;

            CallbackPollingListener listener = new CallbackPollingListener(_ =>
            {
                polls++;
                cancellationTokenSource.Cancel();
                return Task.CompletedTask;
            });

            await listener.Loop(cancellationTokenSource.Token);

            Assert.Equal(1, polls);
        }

        [Fact]
        public async Task StopAsync_CancelsThePollingLoop()
        {
            CallbackPollingListener listener = new CallbackPollingListener(_ => Task.CompletedTask);
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { A.Fake<IServer>() });
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(listener.connection, multiplexer);
            await listener.StartAsync(CancellationToken.None);

            await listener.StopAsync(CancellationToken.None);

            Assert.True(listener.stopTokenSource.IsCancellationRequested);
        }

        private sealed class CallbackPollingListener : RedisPollingTriggerBaseListener
        {
            private readonly Func<CancellationToken, Task> onPoll;

            public CallbackPollingListener(Func<CancellationToken, Task> onPoll)
                : base("name", null, null, Guid.NewGuid().ToString(), "key", TimeSpan.FromMilliseconds(1), 1, false, null, A.Fake<ILogger>())
            {
                this.onPoll = onPoll;
            }

            public override Task PollAsync(CancellationToken cancellationToken)
            {
                return onPoll(cancellationToken);
            }
        }
    }
}
