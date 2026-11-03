using FakeItEasy;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using Xunit;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Unit
{
    public class RedisOptionsTests
    {
        /// <summary>
        /// The host binds an extension's options from the "extensions" section named after the extension, under the
        /// section that AzureWebJobsConfigurationSection names. The Functions host sets that to the section it loads
        /// host.json into, which is what this test reproduces.
        /// </summary>
        [Fact]
        public void AddRedis_BindsTheOptions_FromTheExtensionSection()
        {
            IHost host = new HostBuilder()
                .ConfigureAppConfiguration(configuration => configuration.AddInMemoryCollection(new Dictionary<string, string>
                {
                    { "AzureWebJobsConfigurationSection", "AzureFunctionsJobHost" },
                    { "AzureFunctionsJobHost:extensions:redis:streamClaimMinIdleTimeInMs", "120000" },
                    { "AzureFunctionsJobHost:extensions:redis:streamMaxDeliveryCount", "4" },
                }))
                .ConfigureWebJobs(builder => builder.AddRedis())
                .Build();

            RedisOptions options = host.Services.GetRequiredService<IOptions<RedisOptions>>().Value;

            Assert.Equal(120000, options.StreamClaimMinIdleTimeInMs);
            Assert.Equal(4, options.StreamMaxDeliveryCount);
        }

        [Fact]
        public void AddRedis_LeavesReclaimingOff_WhenTheSectionIsAbsent()
        {
            IHost host = new HostBuilder()
                .ConfigureWebJobs(builder => builder.AddRedis())
                .Build();

            RedisOptions options = host.Services.GetRequiredService<IOptions<RedisOptions>>().Value;

            Assert.Equal(0, options.StreamClaimMinIdleTimeInMs);
            Assert.Equal(0, options.StreamMaxDeliveryCount);
            Assert.False(options.StreamReclaimEnabled);
        }

        [Theory]
        [InlineData(0, 0, false)]
        [InlineData(120000, 4, true)]
        public void StreamReclaimEnabled_RequiresBothValues(int claimMinIdleTimeInMs, int maxDeliveryCount, bool expected)
        {
            RedisOptions options = new RedisOptions { StreamClaimMinIdleTimeInMs = claimMinIdleTimeInMs, StreamMaxDeliveryCount = maxDeliveryCount };

            Assert.Equal(expected, options.StreamReclaimEnabled);
        }

        [Theory]
        [InlineData(120000, 0)]
        [InlineData(0, 4)]
        [InlineData(-1, 4)]
        [InlineData(120000, -1)]
        public void ConfigProvider_Throws_WhenTheOptionsAreInconsistent(int claimMinIdleTimeInMs, int maxDeliveryCount)
        {
            RedisOptions options = new RedisOptions { StreamClaimMinIdleTimeInMs = claimMinIdleTimeInMs, StreamMaxDeliveryCount = maxDeliveryCount };

            Assert.Throws<InvalidOperationException>(() => CreateConfigProvider(options));
        }

        [Theory]
        [InlineData(0, 0)]
        [InlineData(120000, 4)]
        public void ConfigProvider_Accepts_ConsistentOptions(int claimMinIdleTimeInMs, int maxDeliveryCount)
        {
            RedisOptions options = new RedisOptions { StreamClaimMinIdleTimeInMs = claimMinIdleTimeInMs, StreamMaxDeliveryCount = maxDeliveryCount };

            RedisExtensionConfigProvider provider = CreateConfigProvider(options);

            Assert.Same(options, provider.options);
        }

        [Fact]
        public void Format_ListsEveryOption()
        {
            RedisOptions options = new RedisOptions { StreamClaimMinIdleTimeInMs = 120000, StreamMaxDeliveryCount = 4 };

            string formatted = options.Format();

            Assert.Contains("\"StreamClaimMinIdleTimeInMs\": 120000", formatted);
            Assert.Contains("\"StreamMaxDeliveryCount\": 4", formatted);
        }

        private static RedisExtensionConfigProvider CreateConfigProvider(RedisOptions options)
        {
            return new RedisExtensionConfigProvider(A.Fake<IConfiguration>(), A.Fake<INameResolver>(), A.Fake<ILoggerFactory>(), A.Fake<AzureComponentFactory>(), Options.Create(options));
        }
    }
}
