using Microsoft.Azure.WebJobs.Host.Executors;
using Microsoft.Azure.WebJobs.Host.Listeners;
using Microsoft.Azure.WebJobs.Host.Scale;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;


namespace Microsoft.Azure.WebJobs.Extensions.Redis
{
    /// <summary>
    /// Responsible for polling a cache.
    /// </summary>
    internal abstract class RedisPollingTriggerBaseListener : IListener, IScaleMonitorProvider, ITargetScalerProvider
    {
        internal string name;
        internal IConfiguration configuration;
        internal AzureComponentFactory azureComponentFactory;
        internal string connection;
        internal string key;
        internal TimeSpan pollingInterval;
        internal int maxBatchSize;
        internal bool batch;
        internal ITriggeredFunctionExecutor executor;
        internal ILogger logger;

        internal IConnectionMultiplexer multiplexer;
        internal string logPrefix;
        internal Version serverVersion;
        internal RedisPollingTriggerBaseScaleMonitor scaleMonitor;
        internal CancellationTokenSource stopTokenSource;

        public RedisPollingTriggerBaseListener(string name, IConfiguration configuration, AzureComponentFactory azureComponentFactory, string connection, string key, TimeSpan pollingInterval, int maxBatchSize, bool batch, ITriggeredFunctionExecutor executor, ILogger logger)
        {
            this.name = name;
            this.configuration = configuration;
            this.azureComponentFactory = azureComponentFactory;
            this.connection = connection;
            this.key = key;
            this.pollingInterval = pollingInterval;
            this.maxBatchSize = maxBatchSize;
            this.batch = batch;
            this.executor = executor;
            this.logger = logger;
        }

        /// <summary>
        /// Connects to Redis, runs any commands the trigger needs before polling and starts the polling loop.
        /// The loop polls on a token the listener owns and cancels from <see cref="StopAsync"/>, <see cref="Cancel"/>
        /// and <see cref="Dispose"/>. The host's start token would never end it.
        /// </summary>
        public virtual async Task StartAsync(CancellationToken cancellationToken)
        {
            multiplexer = await RedisExtensionConfigProvider.GetOrCreateConnectionMultiplexerAsync(configuration, azureComponentFactory, connection, name);
            logger?.LogInformation($"{logPrefix} Connecting to Redis.");
            serverVersion = multiplexer.GetServers()[0].Version;
            BeforePolling();
            stopTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _ = Task.Run(() => Loop(stopTokenSource.Token));
        }

        /// <summary>
        /// Triggers disconnect from cache when cancellation token is invoked.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            stopTokenSource?.Cancel();
            await CloseMultiplexerAsync(multiplexer);
        }

        public async void Cancel()
        {
            stopTokenSource?.Cancel();
            await CloseMultiplexerAsync(multiplexer);
        }

        public async void Dispose()
        {
            stopTokenSource?.Cancel();
            await CloseMultiplexerAsync(multiplexer);
        }

        /// <summary>
        /// Closes redis cache multiplexer connection.
        /// </summary>
        internal async Task CloseMultiplexerAsync(IConnectionMultiplexer existingMultiplexer)
        {
            BeforeClosing();
            logger?.LogInformation($"{logPrefix} Closing and disposing multiplexer.");
            await existingMultiplexer.CloseAsync();
            await existingMultiplexer.DisposeAsync();
        }

        /// <summary>
        /// Any Redis commands necessary to run after the connection is created but before the polling starts.
        /// </summary>
        public virtual void BeforePolling() { }

        /// <summary>
        /// Implementation of the logic used to poll the cache.
        /// </summary>
        public abstract Task PollAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Any Redis commands necessary to run before the connection is terminated.
        /// </summary>
        public virtual void BeforeClosing() { }

        /// <summary>
        /// Main loop thread.
        /// </summary>
        private async Task Loop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await PollAsync(cancellationToken);
                }
                catch (Exception e)
                {
                    // A transient failure (e.g. a dropped Redis connection or command timeout) must not be
                    // allowed to escape the loop. Because this task is started fire-and-forget, an unhandled
                    // exception here ends the loop and silently stops the listener for the remaining lifetime
                    // of the host process, with no further log output. Log and continue so the listener
                    // resumes on the next poll once the multiplexer reconnects.
                    logger?.LogError(e, $"{logPrefix} Exception while polling; listener will continue polling.");
                }

                await Task.Delay(pollingInterval);
            }
        }

        public IScaleMonitor GetMonitor()
        {
            return scaleMonitor;
        }

        public ITargetScaler GetTargetScaler()
        {
            return scaleMonitor;
        }
    }
}
