using Microsoft.Extensions.Logging;
using System;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class StreamTrigger_ReadOnlyMemory
    {
        private readonly ILogger<StreamTrigger_ReadOnlyMemory> logger;

        public StreamTrigger_ReadOnlyMemory(ILogger<StreamTrigger_ReadOnlyMemory> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(StreamTrigger_ReadOnlyMemory))]
        public void Run(
            [RedisStreamTrigger(TestFunctionHelpers.ConnectionString, nameof(StreamTrigger_ReadOnlyMemory), TestFunctionHelpers.PollingIntervalShort)] ReadOnlyMemory<byte> entry)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(entry));
        }
    }
}
