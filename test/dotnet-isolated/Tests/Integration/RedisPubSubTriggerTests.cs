using Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Integration
{
    [Collection("RedisTriggerTests")]
    public class RedisPubSubTriggerTests
    {
        [Theory]
        [InlineData(nameof(PubSubTrigger_String), typeof(string))]
        [InlineData(nameof(PubSubTrigger_ByteArray), typeof(byte[]))]
        [InlineData(nameof(PubSubTrigger_CustomType), typeof(CustomChannelMessage))]
        public async Task PubSubTrigger_TypeConversions_WorkCorrectly(string functionName, Type parameterType)
        {
            string message = "testValue";

            // Whatever the parameter type, the worker receives the subscription channel, the channel the message
            // arrived on and the message as the JSON the host serializes them to. A custom type is that JSON
            // deserialized.
            string channelMessage = JsonConvert.SerializeObject(new CustomChannelMessage { SubscriptionChannel = functionName, Channel = functionName, Message = message });

            Dictionary<string, int> counts = new Dictionary<string, int>
            {
                { IntegrationTestHelpers.GetExecutedLogValue(functionName), 1 },
                { TestFunctionHelpers.FormatLogValue(parameterType, channelMessage), 1 },
            };

            using (Process redisProcess = IntegrationTestHelpers.StartRedis())
            using (ConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(IntegrationTestHelpers.redisConnectionString))
            using (Process functionsProcess = await IntegrationTestHelpers.StartFunctionAsync(functionName, 7071, counts))
            {
                await multiplexer.GetSubscriber().PublishAsync(RedisChannel.Literal(functionName), message);
                await IntegrationTestHelpers.WaitForCountsAsync(counts);

                await multiplexer.CloseAsync();
                functionsProcess.Kill(entireProcessTree: true);
                IntegrationTestHelpers.StopRedis(redisProcess);
            };
            var incorrect = counts.Where(pair => pair.Value != 0);
            Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
        }
    }
}
