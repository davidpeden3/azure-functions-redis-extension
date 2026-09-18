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
        internal Task loopTask;

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
            if (!multiplexer.IsConnected)
            {
                // With abortConnect=false the multiplexer is returned before a connection exists and reconnects in
                // the background. Reading the server version from an unconnected endpoint yields the configured
                // default rather than the real version. Fail the start and let the host retry it.
                throw new RedisConnectionException(ConnectionFailureType.UnableToConnect, $"{logPrefix} Redis is not connected.");
            }

            serverVersion = multiplexer.GetServers()[0].Version;
            await BeforePollingAsync();
            stopTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            loopTask = Task.Run(() => Loop(stopTokenSource.Token));
        }

        /// <summary>
        /// Ends the polling loop, waits for a poll already in flight to finish and then runs any commands the
        /// trigger needs before the listener goes away. The multiplexer stays open. Every trigger, scale monitor
        /// and binding on the same connection shares it and the cache in <see cref="RedisExtensionConfigProvider"/>
        /// owns it for the life of the process.
        /// </summary>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (loopTask is null)
            {
                return;
            }

            stopTokenSource?.Cancel();
            await loopTask;
            await BeforeClosingAsync();
        }

        public void Cancel()
        {
            stopTokenSource?.Cancel();
        }

        public void Dispose()
        {
            if (stopTokenSource is null)
            {
                return;
            }

            stopTokenSource.Cancel();
            stopTokenSource.Dispose();
            stopTokenSource = null;
        }

        /// <summary>
        /// Any Redis commands necessary to run after the connection is created but before the polling starts.
        /// A failure thrown from here fails the start of the listener so that the host retries it.
        /// </summary>
        public virtual Task BeforePollingAsync()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Implementation of the logic used to poll the cache.
        /// </summary>
        public abstract Task PollAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Any Redis commands necessary to run after the polling loop has ended and before the listener goes away.
        /// </summary>
        public virtual Task BeforeClosingAsync()
        {
            return Task.CompletedTask;
        }

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
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // The listener is stopping. A poll that observed the token is not a failure.
                    return;
                }
                catch (Exception e)
                {
                    // A transient failure (e.g. a dropped Redis connection or command timeout) must not be
                    // allowed to escape the loop. An unhandled exception here ends the loop and silently stops
                    // the listener for the remaining lifetime of the host process, with no further log output.
                    // Log and continue so the listener resumes on the next poll once the multiplexer reconnects.
                    logger?.LogError(e, $"{logPrefix} Exception while polling; listener will continue polling.");
                }

                try
                {
                    await Task.Delay(pollingInterval, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // The listener is stopping. Waking here is what keeps StopAsync from waiting out a whole
                    // polling interval.
                    return;
                }
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
