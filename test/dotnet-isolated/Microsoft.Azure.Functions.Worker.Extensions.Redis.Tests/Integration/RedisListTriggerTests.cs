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
    public class RedisListTriggerTests
    {
        [Theory]
        [InlineData(nameof(ListTrigger_String), typeof(string))]
        [InlineData(nameof(ListTrigger_ByteArray), typeof(byte[]))]
        [InlineData(nameof(ListTrigger_ReadOnlyMemory), typeof(ReadOnlyMemory<byte>))]
        [InlineData(nameof(ListTrigger_CustomType), typeof(CustomType))]
        public async Task ListTrigger_TypeConversions_WorkCorrectly(string functionName, Type parameterType)
        {
            // The worker receives the element as it sits in the list. A custom type is the element deserialized.
            string entry = JsonConvert.SerializeObject(new CustomType { Name = "randomName", Field = "someField", Random = "random" });

            Dictionary<string, int> counts = new Dictionary<string, int>
            {
                { IntegrationTestHelpers.GetExecutedLogValue(functionName), 1 },
                { TestFunctionHelpers.FormatLogValue(parameterType, entry), 1 },
            };

            using (Process redisProcess = IntegrationTestHelpers.StartRedis())
            using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(await RedisUtilities.ResolveConfigurationOptionsAsync(IntegrationTestHelpers.localsettings, null, TestFunctionHelpers.ConnectionString, "test")))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);
                await multiplexer.GetDatabase().ListLeftPushAsync(functionName, entry);

                using (Process functionsProcess = await IntegrationTestHelpers.StartFunctionAsync(functionName, 7071, counts))
                {
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
