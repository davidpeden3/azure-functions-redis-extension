namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    /// <summary>
    /// The shape the pub/sub trigger hands an isolated worker: the subscription channel, the channel the message
    /// arrived on and the message, as the host serializes them. A custom parameter type on the pub/sub trigger is
    /// this JSON deserialized.
    /// </summary>
    public class CustomChannelMessage
    {
        public string SubscriptionChannel { get; set; }
        public string Channel { get; set; }
        public string Message { get; set; }
    }
}
