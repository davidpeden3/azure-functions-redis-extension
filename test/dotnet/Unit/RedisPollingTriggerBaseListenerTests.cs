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
            }, A.Fake<ILogger>());

            await listener.Loop(cancellationTokenSource.Token);

            Assert.True(polls >= 3, $"Expected the loop to keep polling after an exception, but it polled {polls} time(s).");
        }

        private sealed class CallbackPollingListener : RedisPollingTriggerBaseListener
        {
            private readonly Func<CancellationToken, Task> onPoll;

            public CallbackPollingListener(Func<CancellationToken, Task> onPoll, ILogger logger)
                : base("name", null, null, "connection", "key", TimeSpan.FromMilliseconds(1), 1, false, null, logger)
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
