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
        public async Task StartAsync_KeepsPolling_AfterPollAsyncThrows()
        {
            TaskCompletionSource<bool> thirdPoll = new TaskCompletionSource<bool>();
            int polls = 0;

            CallbackPollingListener listener = new CallbackPollingListener(_ =>
            {
                polls++;
                if (polls == 1)
                {
                    // Simulate a transient failure (e.g. a dropped connection or command timeout) on the first poll.
                    throw new RedisException("transient failure");
                }

                if (polls == 3)
                {
                    thirdPoll.TrySetResult(true);
                }

                return Task.CompletedTask;
            });
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { A.Fake<IServer>() });
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(listener.connection, multiplexer);

            await listener.StartAsync(CancellationToken.None);
            await thirdPoll.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await listener.StopAsync(CancellationToken.None);

            Assert.True(polls >= 3, $"Expected the loop to keep polling after an exception. It polled {polls} time(s).");
        }

        [Fact]
        public async Task StopAsync_StopsThePollingLoop()
        {
            TaskCompletionSource<bool> firstPoll = new TaskCompletionSource<bool>();
            int polls = 0;

            CallbackPollingListener listener = new CallbackPollingListener(_ =>
            {
                polls++;
                firstPoll.TrySetResult(true);
                return Task.CompletedTask;
            });
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { A.Fake<IServer>() });
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(listener.connection, multiplexer);
            await listener.StartAsync(CancellationToken.None);
            await firstPoll.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await listener.StopAsync(CancellationToken.None);

            // The loop checks the token before each poll and sleeps one polling interval between polls.
            // A poll already in flight when StopAsync ran can therefore still land. After that, none may.
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            int pollsAfterStop = polls;
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            Assert.Equal(pollsAfterStop, polls);
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
