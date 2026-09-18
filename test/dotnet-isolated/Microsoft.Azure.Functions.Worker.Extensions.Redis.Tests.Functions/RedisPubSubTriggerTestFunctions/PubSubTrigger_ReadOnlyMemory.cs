using Microsoft.Extensions.Logging;
using System;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class PubSubTrigger_ReadOnlyMemory
    {
        private readonly ILogger<PubSubTrigger_ReadOnlyMemory> logger;

        public PubSubTrigger_ReadOnlyMemory(ILogger<PubSubTrigger_ReadOnlyMemory> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(PubSubTrigger_ReadOnlyMemory))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(PubSubTrigger_ReadOnlyMemory))] ReadOnlyMemory<byte> message)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(message));
        }
    }
}
