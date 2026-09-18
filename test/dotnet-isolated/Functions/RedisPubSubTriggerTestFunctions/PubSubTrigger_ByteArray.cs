using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class PubSubTrigger_ByteArray
    {
        private readonly ILogger<PubSubTrigger_ByteArray> logger;

        public PubSubTrigger_ByteArray(ILogger<PubSubTrigger_ByteArray> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(PubSubTrigger_ByteArray))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(PubSubTrigger_ByteArray))] byte[] message)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(message));
        }
    }
}
