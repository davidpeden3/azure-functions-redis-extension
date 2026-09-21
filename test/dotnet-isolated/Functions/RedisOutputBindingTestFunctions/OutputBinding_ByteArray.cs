using Microsoft.Extensions.Logging;
using System.Text;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class OutputBinding_ByteArray
    {
        private readonly ILogger<OutputBinding_ByteArray> logger;

        public OutputBinding_ByteArray(ILogger<OutputBinding_ByteArray> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(OutputBinding_ByteArray))]
        [RedisOutput(TestFunctionHelpers.ConnectionString, "DEL")]
        public byte[] Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(OutputBinding_ByteArray))] CustomChannelMessage message)
        {
            logger.LogInformation("Deleting key '{Key}'", message.Message);
            return Encoding.UTF8.GetBytes(message.Message);
        }
    }
}
