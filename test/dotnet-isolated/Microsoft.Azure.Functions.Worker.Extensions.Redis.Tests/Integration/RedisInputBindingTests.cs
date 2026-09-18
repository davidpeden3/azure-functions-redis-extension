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
    public class RedisInputBindingTests
    {
        [Theory]
        [InlineData(nameof(InputBinding_String), typeof(string))]
        [InlineData(nameof(InputBinding_ByteArray), typeof(byte[]))]
        [InlineData(nameof(InputBinding_ReadOnlyMemory), typeof(ReadOnlyMemory<byte>))]
        [InlineData(nameof(InputBinding_CustomType), typeof(CustomType))]
        public async Task InputBinding_TypeConversions_WorkCorrectly(string functionName, Type parameterType)
        {
            // The worker receives the value as it sits in the key. A custom type is the value deserialized.
            string value = JsonConvert.SerializeObject(new CustomType { Name = "randomName", Field = "someField", Random = "random" });

            Dictionary<string, int> counts = new Dictionary<string, int>
            {
                { IntegrationTestHelpers.GetExecutedLogValue(functionName), 1 },
                { TestFunctionHelpers.FormatLogValue(parameterType, value), 1 },
            };

            using (Process redisProcess = IntegrationTestHelpers.StartRedis())
            using (ConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(IntegrationTestHelpers.redisConnectionString))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);
                await multiplexer.GetDatabase().StringSetAsync(functionName, value);

                using (Process functionsProcess = await IntegrationTestHelpers.StartFunctionAsync(functionName, 7071, counts))
                {
                    await multiplexer.GetSubscriber().PublishAsync(RedisChannel.Literal(functionName), "start");
                    await Task.Delay(TimeSpan.FromSeconds(1));

                    await multiplexer.CloseAsync();
                    functionsProcess.Kill(entireProcessTree: true);
                    IntegrationTestHelpers.StopRedis(redisProcess);
                };
            }
            var incorrect = counts.Where(pair => pair.Value != 0);
            Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
        }
    }
}
