using Microsoft.Extensions.Logging;
using System;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class InputBinding_ReadOnlyMemory
    {
        private readonly ILogger<InputBinding_ReadOnlyMemory> logger;

        public InputBinding_ReadOnlyMemory(ILogger<InputBinding_ReadOnlyMemory> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(InputBinding_ReadOnlyMemory))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(InputBinding_ReadOnlyMemory))] string message,
            [RedisInput(TestFunctionHelpers.ConnectionString, "GET " + nameof(InputBinding_ReadOnlyMemory))] ReadOnlyMemory<byte> value)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(value));
        }
    }
}
