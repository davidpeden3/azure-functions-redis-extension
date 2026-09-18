using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class OutputBinding_String
    {
        private readonly ILogger<OutputBinding_String> logger;

        public OutputBinding_String(ILogger<OutputBinding_String> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(OutputBinding_String))]
        [RedisOutput(TestFunctionHelpers.ConnectionString, "DEL")]
        public string Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(OutputBinding_String))] CustomChannelMessage message)
        {
            logger.LogInformation("Deleting key '{Key}'", message.Message);
            return message.Message;
        }
    }
}
