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
    public class RedisStreamTriggerTests
    {
        [Theory]
        [InlineData(nameof(StreamTrigger_String), typeof(string))]
        [InlineData(nameof(StreamTrigger_ByteArray), typeof(byte[]))]
        [InlineData(nameof(StreamTrigger_CustomType), typeof(CustomStreamEntry))]
        public async Task StreamTrigger_TypeConversions_WorkCorrectly(string functionName, Type parameterType)
        {
            NameValueEntry[] nameValueEntries = new NameValueEntry[]
            {
                new NameValueEntry(nameof(CustomType.Name), "randomName"),
                new NameValueEntry(nameof(CustomType.Field), "someField"),
            };

            Dictionary<string, int> counts = new Dictionary<string, int>
            {
                { IntegrationTestHelpers.GetExecutedLogValue(functionName), 1 },
            };

            using (Process redisProcess = IntegrationTestHelpers.StartRedis())
            using (ConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(IntegrationTestHelpers.redisConnectionString))
            {
                await multiplexer.GetDatabase().KeyDeleteAsync(functionName);
                RedisValue id = await multiplexer.GetDatabase().StreamAddAsync(functionName, nameValueEntries);

                // Whatever the parameter type, the worker receives the entry as the JSON the host serializes it to:
                // its id and its fields. A custom type is that JSON deserialized.
                string entry = JsonConvert.SerializeObject(new CustomStreamEntry
                {
                    Id = id.ToString(),
                    Values = nameValueEntries.ToDictionary(value => value.Name.ToString(), value => value.Value.ToString()),
                });
                counts.Add(TestFunctionHelpers.FormatLogValue(parameterType, entry), 1);

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
