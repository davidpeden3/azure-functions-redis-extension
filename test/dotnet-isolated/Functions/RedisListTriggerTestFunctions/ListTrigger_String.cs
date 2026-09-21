using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class ListTrigger_String
    {
        private readonly ILogger<ListTrigger_String> logger;

        public ListTrigger_String(ILogger<ListTrigger_String> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(ListTrigger_String))]
        public void Run(
            [RedisListTrigger(TestFunctionHelpers.ConnectionString, nameof(ListTrigger_String), TestFunctionHelpers.PollingIntervalShort)] string entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
