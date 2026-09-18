using Newtonsoft.Json;
using System;
using System.Text;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    public static class TestFunctionHelpers
    {
        public const string ConnectionString = "redisConnectionString";
        public const int PollingIntervalShort = 100;

        /// <summary>
        /// The line a test function logs for the value it was invoked with: the parameter type and the value as
        /// text. A test then asserts on what the worker bound, not only that it bound something.
        /// </summary>
        public static string GetLogValue(object value)
        {
            switch (value)
            {
                case string text:
                    return FormatLogValue(typeof(string), text);
                case byte[] bytes:
                    return FormatLogValue(typeof(byte[]), Encoding.UTF8.GetString(bytes));
                default:
                    return FormatLogValue(value.GetType(), JsonConvert.SerializeObject(value));
            }
        }

        public static string FormatLogValue(Type parameterType, string text)
        {
            return $"{parameterType.FullName}:{text}";
        }
    }
}
