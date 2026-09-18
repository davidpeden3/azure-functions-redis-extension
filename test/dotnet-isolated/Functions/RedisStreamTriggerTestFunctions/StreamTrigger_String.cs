using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class StreamTrigger_String
    {
        private readonly ILogger<StreamTrigger_String> logger;

        public StreamTrigger_String(ILogger<StreamTrigger_String> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(StreamTrigger_String))]
        public void Run(
            [RedisStreamTrigger(TestFunctionHelpers.ConnectionString, nameof(StreamTrigger_String), TestFunctionHelpers.PollingIntervalShort)] string entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
