using System.Collections.Generic;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    /// <summary>
    /// The shape the stream trigger hands an isolated worker: the entry's id and its fields, as the host
    /// serializes them. A custom parameter type on the stream trigger is this JSON deserialized.
    /// </summary>
    public class CustomStreamEntry
    {
        public string Id { get; set; }
        public Dictionary<string, string> Values { get; set; }
    }
}
