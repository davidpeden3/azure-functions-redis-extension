using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class InputBinding_CustomType
    {
        private readonly ILogger<InputBinding_CustomType> logger;

        public InputBinding_CustomType(ILogger<InputBinding_CustomType> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(InputBinding_CustomType))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(InputBinding_CustomType))] string message,
            [RedisInput(TestFunctionHelpers.ConnectionString, "GET " + nameof(InputBinding_CustomType))] CustomType value)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(value));
        }
    }
}
