using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class StreamTrigger_ByteArray
    {
        private readonly ILogger<StreamTrigger_ByteArray> logger;

        public StreamTrigger_ByteArray(ILogger<StreamTrigger_ByteArray> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(StreamTrigger_ByteArray))]
        public void Run(
            [RedisStreamTrigger(TestFunctionHelpers.ConnectionString, nameof(StreamTrigger_ByteArray), TestFunctionHelpers.PollingIntervalShort)] byte[] entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
