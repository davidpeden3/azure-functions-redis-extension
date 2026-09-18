using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class PubSubTrigger_String
    {
        private readonly ILogger<PubSubTrigger_String> logger;

        public PubSubTrigger_String(ILogger<PubSubTrigger_String> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(PubSubTrigger_String))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(PubSubTrigger_String))] string message)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(message));
        }
    }
}
