using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class StreamTrigger_CustomType
    {
        private readonly ILogger<StreamTrigger_CustomType> logger;

        public StreamTrigger_CustomType(ILogger<StreamTrigger_CustomType> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(StreamTrigger_CustomType))]
        public void Run(
            [RedisStreamTrigger(TestFunctionHelpers.ConnectionString, nameof(StreamTrigger_CustomType), TestFunctionHelpers.PollingIntervalShort)] CustomStreamEntry entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
