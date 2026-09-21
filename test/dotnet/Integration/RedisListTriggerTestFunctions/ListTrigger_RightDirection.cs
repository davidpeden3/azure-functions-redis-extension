using Microsoft.Extensions.Logging;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    public static class ListTrigger_RightDirection
    {
        [FunctionName(nameof(ListTrigger_RightDirection))]
        public static void Run(
            // One entry per poll. The test then proves the direction on every Redis version. Servers from 6.2 pop
            // the whole batch in one poll and would otherwise return both ends of the list at once.
            [RedisListTrigger(IntegrationTestHelpers.ConnectionString, nameof(ListTrigger_RightDirection), IntegrationTestHelpers.PollingIntervalLong, maxBatchSize: 1, listDirection: ListDirection.RIGHT)] string entry,
            ILogger logger)
        {
            logger.LogInformation(IntegrationTestHelpers.GetLogValue(entry));
        }
    }
}
