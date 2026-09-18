using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class InputBinding_ByteArray
    {
        private readonly ILogger<InputBinding_ByteArray> logger;

        public InputBinding_ByteArray(ILogger<InputBinding_ByteArray> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(InputBinding_ByteArray))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(InputBinding_ByteArray))] string message,
            [RedisInput(TestFunctionHelpers.ConnectionString, "GET " + nameof(InputBinding_ByteArray))] byte[] value)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(value));
        }
    }
}
