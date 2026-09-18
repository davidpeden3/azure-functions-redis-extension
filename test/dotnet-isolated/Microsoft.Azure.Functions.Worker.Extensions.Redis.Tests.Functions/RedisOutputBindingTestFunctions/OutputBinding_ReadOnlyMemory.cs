using Microsoft.Extensions.Logging;
using System;
using System.Text;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public class OutputBinding_ReadOnlyMemory
    {
        private readonly ILogger<OutputBinding_ReadOnlyMemory> logger;

        public OutputBinding_ReadOnlyMemory(ILogger<OutputBinding_ReadOnlyMemory> logger)
        {
            this.logger = logger;
        }

        [Function(nameof(OutputBinding_ReadOnlyMemory))]
        [RedisOutput(TestFunctionHelpers.ConnectionString, "DEL")]
        public ReadOnlyMemory<byte> Run(
            [RedisPubSubTrigger(TestFunctionHelpers.ConnectionString, nameof(OutputBinding_ReadOnlyMemory))] CustomChannelMessage message)
        {
            logger.LogInformation("Deleting key '{Key}'", message.Message);
            return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(message.Message));
        }
    }
}
