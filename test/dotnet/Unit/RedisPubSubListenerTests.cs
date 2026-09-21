using FakeItEasy;
using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Unit
{
    public class RedisPubSubListenerTests
    {
        [Fact]
        public async Task StopAsync_LeavesTheMultiplexerOpen()
        {
            IConnectionMultiplexer multiplexer = A.Fake<IConnectionMultiplexer>();
            RedisPubSubListener listener = new RedisPubSubListener("name", multiplexer, "channel", false, A.Fake<ITriggeredFunctionExecutor>(), A.Fake<ILogger>());

            await listener.StopAsync(CancellationToken.None);

            A.CallTo(() => multiplexer.CloseAsync(A<bool>._)).MustNotHaveHappened();
            A.CallTo(() => multiplexer.DisposeAsync()).MustNotHaveHappened();
        }
    }
}
