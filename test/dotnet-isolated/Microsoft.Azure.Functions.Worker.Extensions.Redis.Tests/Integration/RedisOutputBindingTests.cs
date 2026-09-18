using Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions;
using Microsoft.Azure.WebJobs.Extensions.Redis;
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
    public class RedisOutputBindingTests
    {
        [Theory]
        [InlineData(nameof(OutputBinding_String))]
        [InlineData(nameof(OutputBinding_ByteArray))]
        [InlineData(nameof(OutputBinding_ReadOnlyMemory))]
        public async Task OutputBinding_TypeConversions_WorkCorrectly(string functionName)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>
            {
                { IntegrationTestHelpers.GetExecutedLogValue(functionName), 1 },
            };

            bool exists = true;
            using (Process redisProcess = IntegrationTestHelpers.StartRedis())
            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(await RedisUtilities.ResolveConfigurationOptionsAsync(IntegrationTestHelpers.localsettings, null, TestFunctionHelpers.ConnectionString, "test")))
            {
                await multiplexer.GetDatabase().StringSetAsync(functionName, "test");

                using (Process functionsProcess = await IntegrationTestHelpers.StartFunctionAsync(functionName, 7071, counts))
                {
                    // The function deletes the key named in the message.
                    await multiplexer.GetSubscriber().PublishAsync(RedisChannel.Literal(functionName), functionName);
                    await Task.Delay(TimeSpan.FromSeconds(1));

                    exists = await multiplexer.GetDatabase().KeyExistsAsync(functionName);
                    await multiplexer.CloseAsync();
                    functionsProcess.Kill(entireProcessTree: true);
                    IntegrationTestHelpers.StopRedis(redisProcess);
                };
            }
            var incorrect = counts.Where(pair => pair.Value != 0);
            Assert.False(incorrect.Any(), JsonConvert.SerializeObject(incorrect));
            Assert.False(exists);
        }
    }
}
