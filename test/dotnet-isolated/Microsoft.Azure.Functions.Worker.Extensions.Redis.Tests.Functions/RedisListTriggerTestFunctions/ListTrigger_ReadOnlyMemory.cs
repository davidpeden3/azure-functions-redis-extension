using Microsoft.Extensions.Logging;
using System;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class ListTrigger_ReadOnlyMemory
    {
        private readonly ILogger<ListTrigger_ReadOnlyMemory> logger;

        public ListTrigger_ReadOnlyMemory(ILogger<ListTrigger_ReadOnlyMemory> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(ListTrigger_ReadOnlyMemory))]
        public void Run(
            [RedisListTrigger(TestFunctionHelpers.ConnectionString, nameof(ListTrigger_ReadOnlyMemory), TestFunctionHelpers.PollingIntervalShort)] ReadOnlyMemory<byte> entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
