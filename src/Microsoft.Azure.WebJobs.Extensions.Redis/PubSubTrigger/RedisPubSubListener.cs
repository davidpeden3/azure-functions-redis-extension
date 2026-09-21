using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Extensions.Redis
{
    /// <summary>
    /// Responsible for managing connections and listening to a given Redis instance.
    /// </summary>
    internal sealed class RedisPubSubListener : IListener
    {
        internal IConnectionMultiplexer multiplexer;
        internal string channel;
        internal bool pattern;
        internal ITriggeredFunctionExecutor executor;
        internal ILogger logger;
        internal string logPrefix;
        internal ChannelMessageQueue channelMessageQueue;

        public RedisPubSubListener(string name, IConnectionMultiplexer multiplexer, string channel, bool pattern, ITriggeredFunctionExecutor executor, ILogger logger)
        {
            this.multiplexer = multiplexer;
            this.channel = channel;
            this.pattern = pattern;
            this.executor = executor;
            this.logger = logger;
            this.logPrefix = $"[Name:{name}][Trigger:RedisPubSubTrigger][Channel:{channel}]";
        }

        /// <summary>
        /// Executes enabled functions, primary listener method.
        /// </summary>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            RedisChannel redisChannel = new RedisChannel(channel, pattern ? RedisChannel.PatternMode.Pattern : RedisChannel.PatternMode.Literal);
            channelMessageQueue = await multiplexer.GetSubscriber().SubscribeAsync(redisChannel);
            channelMessageQueue.OnMessage(async (message) =>
            {
                logger?.LogDebug($"{logPrefix} Message received on channel '{channel}'.");
                await executor.TryExecuteAsync(new TriggeredFunctionData() { TriggerValue = message }, cancellationToken);
            });
            logger?.LogInformation($"{logPrefix} Subscribed to channel '{channel}'.");
        }

        /// <summary>
        /// Unsubscribes from the channel. The multiplexer stays open. Every trigger, scale monitor and binding on
        /// the same connection shares it and the cache in <see cref="RedisExtensionConfigProvider"/> owns it for
        /// the life of the process.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (channelMessageQueue is null)
            {
                return;
            }

            await channelMessageQueue.UnsubscribeAsync();
            logger?.LogInformation($"{logPrefix} Unsubscribed from channel '{channel}'.");
        }

        public void Cancel()
        {
            channelMessageQueue?.Unsubscribe();
        }

        public void Dispose()
        {
            channelMessageQueue?.Unsubscribe();
        }
    }
}
