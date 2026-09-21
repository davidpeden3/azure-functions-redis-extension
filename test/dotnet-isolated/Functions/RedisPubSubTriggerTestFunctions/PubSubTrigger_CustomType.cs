using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class PubSubTrigger_CustomType
    {
        private readonly ILogger<PubSubTrigger_CustomType> logger;

        public PubSubTrigger_CustomType(ILogger<PubSubTrigger_CustomType> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(PubSubTrigger_CustomType))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(PubSubTrigger_CustomType))] CustomChannelMessage message)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(message));
        }
    }
}
