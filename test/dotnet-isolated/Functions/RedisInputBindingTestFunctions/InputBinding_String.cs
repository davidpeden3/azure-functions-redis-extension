using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class InputBinding_String
    {
        private readonly ILogger<InputBinding_String> logger;

        public InputBinding_String(ILogger<InputBinding_String> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(InputBinding_String))]
        public void Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(InputBinding_String))] string message,
            [RedisInput(TestFunctionHelpers.ConnectionString, "GET " + nameof(InputBinding_String))] string value)
        {
            logger.LogInformation("{LogValue}", TestFunctionHelpers.GetLogValue(value));
        }
    }
}
