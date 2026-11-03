using Microsoft.Azure.WebJobs.Hosting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.Redis
{
    /// <summary>
    /// Options for the Redis extension, bound from the <c>extensions:redis</c> section of host.json.
    /// </summary>
    public class RedisOptions : IOptionsFormatter
    {
        /// <summary>
        /// How long, in milliseconds, an entry must have been pending in a stream trigger's consumer group before
        /// the trigger reclaims it and runs the function again. Default: 0, which leaves pending entries alone.
        /// </summary>
        public int StreamClaimMinIdleTimeInMs { get; set; }

        /// <summary>
        /// The number of times a stream entry may be delivered to its consumer group. An entry that has been
        /// delivered this many times stays pending and is never reclaimed again. Default: 0, which leaves pending
        /// entries alone.
        /// </summary>
        public int StreamMaxDeliveryCount { get; set; }

        /// <summary>
        /// Whether stream triggers reclaim idle pending entries. Reclaiming needs both an idle time and a delivery
        /// limit. It is on only when both are set.
        /// </summary>
        internal bool StreamReclaimEnabled => StreamClaimMinIdleTimeInMs > 0 && StreamMaxDeliveryCount > 0;

        /// <summary>
        /// Whether the options are consistent: neither value is negative and the two are set together or not at all.
        /// </summary>
        internal bool IsValid => StreamClaimMinIdleTimeInMs >= 0 && StreamMaxDeliveryCount >= 0 && (StreamClaimMinIdleTimeInMs > 0) == (StreamMaxDeliveryCount > 0);

        /// <summary>
        /// Formats the options for the host's startup log.
        /// </summary>
        public string Format()
        {
            JObject options = new JObject
            {
                { nameof(StreamClaimMinIdleTimeInMs), StreamClaimMinIdleTimeInMs },
                { nameof(StreamMaxDeliveryCount), StreamMaxDeliveryCount },
            };
            return options.ToString(Formatting.Indented);
        }
    }
}
