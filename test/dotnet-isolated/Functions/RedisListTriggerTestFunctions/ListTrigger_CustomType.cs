using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class ListTrigger_CustomType
    {
        private readonly ILogger<ListTrigger_CustomType> logger;

        public ListTrigger_CustomType(ILogger<ListTrigger_CustomType> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(ListTrigger_CustomType))]
        public void Run(
            [RedisListTrigger(TestFunctionHelpers.ConnectionString, nameof(ListTrigger_CustomType), TestFunctionHelpers.PollingIntervalShort)] CustomType entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
