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
            SeedConnectedMultiplexer(listener.connection);

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
            SeedConnectedMultiplexer(listener.connection);
            await listener.StartAsync(CancellationToken.None);
            await firstPoll.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await listener.StopAsync(CancellationToken.None);

            Assert.True(listener.loopTask.IsCompleted);
            int pollsAfterStop = polls;
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            Assert.Equal(pollsAfterStop, polls);
        }

        [Fact]
        public async Task StopAsync_WaitsForThePollInFlight()
        {
            TaskCompletionSource<bool> pollStarted = new TaskCompletionSource<bool>();
            TaskCompletionSource<bool> releasePoll = new TaskCompletionSource<bool>();

            CallbackPollingListener listener = new CallbackPollingListener(async _ =>
            {
                pollStarted.TrySetResult(true);
                await releasePoll.Task;
            });
            SeedConnectedMultiplexer(listener.connection);
            await listener.StartAsync(CancellationToken.None);
            await pollStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

            Task stop = listener.StopAsync(CancellationToken.None);
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            Assert.False(stop.IsCompleted);

            releasePoll.SetResult(true);
            await stop.WaitAsync(TimeSpan.FromSeconds(5));
        }

        [Fact]
        public async Task StopAsync_LeavesTheMultiplexerOpen()
        {
            CallbackPollingListener listener = new CallbackPollingListener(_ => Task.CompletedTask);
            IConnectionMultiplexer multiplexer = SeedConnectedMultiplexer(listener.connection);
            await listener.StartAsync(CancellationToken.None);

            await listener.StopAsync(CancellationToken.None);

            A.CallTo(() => multiplexer.CloseAsync(A<bool>._)).MustNotHaveHappened();
            A.CallTo(() => multiplexer.DisposeAsync()).MustNotHaveHappened();
        }

        [Fact]
        public async Task StopAsync_DoesNothing_WhenTheListenerNeverStarted()
        {
            CallbackPollingListener listener = new CallbackPollingListener(_ => Task.CompletedTask);

            await listener.StopAsync(CancellationToken.None);

            Assert.Null(listener.loopTask);
        }

        private static IConnectionMultiplexer SeedConnectedMultiplexer(string connection)
        {
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            A.CallTo(() => multiplexer.IsConnected).Returns(true);
            A.CallTo(() => multiplexer.GetServers()).Returns(new[] { A.Fake<IServer>() });
            RedisExtensionConfigProvider.connectionMultiplexerCache.TryAdd(connection, new Lazy<Task<IConnectionMultiplexer>>(() => Task.FromResult(multiplexer)));
            return multiplexer;
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
