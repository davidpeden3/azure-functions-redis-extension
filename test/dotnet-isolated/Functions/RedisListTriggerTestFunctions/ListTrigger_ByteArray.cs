using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class ListTrigger_ByteArray
    {
        private readonly ILogger<ListTrigger_ByteArray> logger;

        public ListTrigger_ByteArray(ILogger<ListTrigger_ByteArray> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(ListTrigger_ByteArray))]
        public void Run(
            [RedisListTrigger(TestFunctionHelpers.ConnectionString, nameof(ListTrigger_ByteArray), TestFunctionHelpers.PollingIntervalShort)] byte[] entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
