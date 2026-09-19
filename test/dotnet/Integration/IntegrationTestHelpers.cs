using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.Extensions.Redis.Tests.Integration
{
    internal static class IntegrationTestHelpers
    {
        internal const int PollingIntervalShort = 100;
        internal const int PollingIntervalLong = 10000;
        internal const int BatchSize = 10;
        internal const string format = "triggerValue:{0}";
        internal const string PubSubChannel = "testChannel";
        internal const string PubSubMultiple = "testChannel*";
        internal const string KeyspaceChannel = "__keyspace@0__:testKey";
        internal const string KeyspaceMultiple = "__keyspace@0__:testKey*";
        internal const string KeyeventChannelSet = "__keyevent@0__:set";
        internal const string KeyeventChannelAll = "__keyevent@0__:*";
        internal const string KeyspaceChannelAll = "__keyspace@0__:*";
        internal const string AllChannels = "*";

        internal const string ConnectionString = "redisConnectionString";
        internal const string ManagedIdentity = "redisManagedIdentity";
        internal const string Redis60 = "/redis/redis-6.0.20";
        internal const string Redis62 = "/redis/redis-6.2.14";
        internal const string Redis70 = "/redis/redis-7.0.14";

        // Environment variables that override where the harness finds its tools. Set them when the tool is
        // installed somewhere the test runner's PATH does not reach, which is common under an IDE.
        internal const string FunctionsCoreToolsVariable = "AZURE_FUNCTIONS_CORE_TOOLS";
        internal const string RedisServerVariable = "REDIS_SERVER";

        // Overrides the Redis connection string in local.settings.json for both the tests and the Functions
        // host they start. Point it at a server that is already running, such as a dedicated test instance on
        // another machine. The harness then uses that server instead of spawning one. The server is flushed
        // before every test. It must be dedicated to these tests.
        internal const string RedisConnectionStringVariable = "REDIS_CONNECTION_STRING";

        // Where the harness writes each Functions host's full output, relative to the build output.
        internal const string HostLogDirectory = "func-logs";
        private static int hostLogSequence;

        // How long a test waits for the host to produce the output it expects once the host is up.
        internal static readonly TimeSpan OutputTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Starts a Functions host that serves only <paramref name="functionName"/> and counts down
        /// <paramref name="counts"/> as the host's output arrives. The counter is attached once the host is up.
        /// Some tests count words as short as "set" and "del", which the host's own startup output also carries.
        /// </summary>
        internal static async Task<Process> StartFunctionAsync(string functionName, int port, IDictionary<string, int> counts, bool managedIdentity = false)
        {
            // func runs from the build output rather than from the project with --prefix. Core Tools applies
            // --prefix once in the parent process and then launches the in-process host as a child with the
            // same command line from the directory it just moved to. The child applies --prefix a second
            // time and fails on a path that does not exist.
            ProcessStartInfo info = new ProcessStartInfo
            {
                FileName = GetFunctionsFileName(),
                Arguments = $"start --verbose --functions {functionName} --port {port} --no-build",
                WindowStyle = ProcessWindowStyle.Hidden,
                WorkingDirectory = outputDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            info.EnvironmentVariables["FUNCTIONS_RUNTIME_SCALE_MONITORING_ENABLED"] = "1";
            // Core Tools injects local.settings.json into the host's environment under this prefix and leaves
            // any variable that already exists alone. This keeps the host on the same server as the test.
            info.EnvironmentVariables[ConnectionString] = redisConnectionString;
            Process functionsProcess = new Process() { StartInfo = info };

            // The last lines the host wrote. A start that fails then says why instead of only that it did.
            Queue<string> outputTail = new Queue<string>();
            void outputTailHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data == null)
                {
                    return;
                }

                lock (outputTail)
                {
                    outputTail.Enqueue(e.Data);
                    if (outputTail.Count > 50)
                    {
                        outputTail.Dequeue();
                    }
                }
            }
            functionsProcess.OutputDataReceived += outputTailHandler;
            functionsProcess.ErrorDataReceived += outputTailHandler;

            // Everything the host writes, kept for the life of the process. A failed assertion can then be read
            // against what the host actually did. One file per host under the build output.
            // The sequence number keeps a host's file distinct from the previous test's host for the same function
            // and port, whose file is still open until its Exited event runs.
            Directory.CreateDirectory(Path.Combine(outputDirectory, HostLogDirectory));
            StreamWriter hostLog = new StreamWriter(Path.Combine(outputDirectory, HostLogDirectory, $"{functionName}-{port}-{Interlocked.Increment(ref hostLogSequence):D3}.log"), append: false) { AutoFlush = true };
            bool hostLogClosed = false;
            void hostLogHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data == null)
                {
                    return;
                }

                lock (hostLog)
                {
                    if (!hostLogClosed)
                    {
                        hostLog.WriteLine(e.Data);
                    }
                }
            }
            functionsProcess.OutputDataReceived += hostLogHandler;
            functionsProcess.ErrorDataReceived += hostLogHandler;
            functionsProcess.EnableRaisingEvents = true;
            functionsProcess.Exited += (sender, e) =>
            {
                lock (hostLog)
                {
                    hostLogClosed = true;
                    hostLog.Dispose();
                }
            };

            string DescribeOutput()
            {
                lock (outputTail)
                {
                    return string.Join(Environment.NewLine, outputTail);
                }
            }

            TaskCompletionSource<bool> hostStarted = new TaskCompletionSource<bool>();
            void hostStartupHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data?.Contains($"Host started") ?? false)
                {
                    hostStarted.SetResult(true);
                }
            }
            functionsProcess.OutputDataReceived += hostStartupHandler;

            TaskCompletionSource<bool> functionLoaded = new TaskCompletionSource<bool>();
            void functionLoadedHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data?.Contains($"Generating 1 job function(s)") ?? false)
                {
                    functionLoaded.SetResult(true);
                }
            }
            functionsProcess.OutputDataReceived += functionLoadedHandler;

            functionsProcess.Start();
            functionsProcess.BeginOutputReadLine();
            functionsProcess.BeginErrorReadLine();
            if (!hostStarted.Task.Wait(TimeSpan.FromMinutes(1)))
            {
                functionsProcess.Kill(entireProcessTree: true);
                throw new Exception($"Azure Functions Host did not start. Its output ended with:{Environment.NewLine}{DescribeOutput()}");
            }
            if (!functionLoaded.Task.Wait(TimeSpan.FromMinutes(1)))
            {
                functionsProcess.Kill(entireProcessTree: true);
                throw new Exception($"Did not load Function {functionName}. The host's output ended with:{Environment.NewLine}{DescribeOutput()}");
            }
            functionsProcess.OutputDataReceived -= hostStartupHandler;
            functionsProcess.OutputDataReceived -= functionLoadedHandler;
            functionsProcess.OutputDataReceived -= outputTailHandler;
            functionsProcess.ErrorDataReceived -= outputTailHandler;

            // Ensure that the client name is correctly set
            ConfigurationOptions options = await RedisUtilities.ResolveConfigurationOptionsAsync(localsettings, new ClientSecretCredentialComponentFactory(), managedIdentity ? ManagedIdentity : ConnectionString, nameof(IntegrationTestHelpers));
            options.AllowAdmin = true;
            IConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
            ClientInfo[] clients = multiplexer.GetServers()[0].ClientList();
            if (!clients.Any(client => client.Name == RedisUtilities.GetRedisClientName(functionName)))
            {
                functionsProcess.Kill(entireProcessTree: true);
                throw new Exception("Function client not found on redis server.");
            }

            functionsProcess.OutputDataReceived += CounterHandlerCreator(counts);
            return functionsProcess;
        }

        /// <summary>
        /// Whether a test will run against a server that takes the same code paths as the Redis build at
        /// <paramref name="versionPath"/>. The extension branches on server version at 6.2 and at 7.0. A
        /// server exercises a build's paths when it sits in the same band. Tests whose assertions depend on a
        /// band skip when the server is in another one. Every other test runs against whichever server
        /// <see cref="StartRedis"/> resolves.
        /// </summary>
        internal static bool HasRedisBuild(string versionPath)
        {
            Version listening = GetListeningRedisVersion();
            if (listening != null)
            {
                return GetVersionBand(listening) == GetVersionBand(GetBuildVersion(versionPath));
            }

            // On Windows the build lives inside WSL, where this process cannot see it.
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) || File.Exists(GetRedisBuildFileName(versionPath));
        }

        private static Version GetBuildVersion(string versionPath)
        {
            // "/redis/redis-6.2.14" names the build it holds.
            return Version.Parse(versionPath.Substring(versionPath.LastIndexOf('-') + 1));
        }

        private static int GetVersionBand(Version version)
        {
            return version >= RedisUtilities.Version70 ? 2 : version >= RedisUtilities.Version62 ? 1 : 0;
        }

        /// <summary>
        /// Gives the test an empty Redis server at the configured endpoint. When a server is already listening
        /// there it is flushed and reused and nothing is returned to stop. Otherwise one is spawned for the test.
        /// </summary>
        internal static Process StartRedis(string versionPath)
        {
            if (TryResetListeningRedis())
            {
                return null;
            }

            ProcessStartInfo info = GetRedisServerStartInfo(versionPath);
            Process redisProcess = new Process() { StartInfo = info };

            TaskCompletionSource<bool> hostStarted = new TaskCompletionSource<bool>();
            void hostStartupHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data?.Contains("* Ready to accept connections") ?? false)
                {
                    hostStarted.SetResult(true);
                }
            }
            redisProcess.OutputDataReceived += hostStartupHandler;

            redisProcess.Start();
            redisProcess.BeginOutputReadLine();
            redisProcess.BeginErrorReadLine();
            if (!hostStarted.Task.Wait(TimeSpan.FromMinutes(1)))
            {
                StopRedis(redisProcess);
                throw new Exception("Redis did not start");
            }
            return redisProcess;
        }

        internal static void StopRedis(Process redis)
        {
            if (redis == null)
            {
                // The test ran against a server that was already listening. It stays up.
                return;
            }

            if (!redis.StartInfo.FileName.EndsWith("wsl.exe", StringComparison.OrdinalIgnoreCase))
            {
                redis.Kill(entireProcessTree: true);
            }
            else
            {
                ProcessStartInfo info = new ProcessStartInfo
                {
                    FileName = @"C:\Windows\System32\wsl.exe",
                    Arguments = "pkill redis-server",
                    WindowStyle = ProcessWindowStyle.Hidden,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false
                };
                Process redisKillProcess = new Process() { StartInfo = info };
                redisKillProcess.Start();
                redisKillProcess.WaitForExit();
            }
        }

        internal static DataReceivedEventHandler CounterHandlerCreator(IDictionary<string, int> counts)
        {
            return (object sender, DataReceivedEventArgs e) =>
            {
                if (e.Data == null)
                {
                    return;
                }

                // Output from several hosts arrives on several threads at once. The decrement is a read and a
                // write. Without the lock decrements are lost and a count ends above zero.
                lock (counts)
                {
                    foreach (string key in counts.Keys.ToList())
                    {
                        counts[key] -= CountOccurrences(e.Data, key);
                    }

                    Monitor.PulseAll(counts);
                }
            };
        }

        /// <summary>
        /// Waits until every count in <paramref name="counts"/> has been driven to zero by the host's output,
        /// then one more polling interval or two. A count that reaches zero proves the host did what the test
        /// expects. The interval after that is the chance for it to do more than the test expects, which is
        /// what a count below zero records and what the batch and scaled-out tests exist to catch. The wait is
        /// bounded. A host that never produces the output fails the test with what is still owed.
        /// </summary>
        internal static async Task WaitForCountsAsync(IDictionary<string, int> counts)
        {
            await Task.Run(() =>
            {
                lock (counts)
                {
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    while (counts.Values.Any(count => count > 0))
                    {
                        TimeSpan remaining = OutputTimeout - stopwatch.Elapsed;
                        if (remaining <= TimeSpan.Zero || !Monitor.Wait(counts, remaining))
                        {
                            throw new TimeoutException($"The host did not produce the expected output within {OutputTimeout}. Still expected: {string.Join(", ", counts.Where(pair => pair.Value > 0).Select(pair => $"{pair.Value} x '{pair.Key}'"))}. The host's full output is under '{HostLogDirectory}' in the build output.");
                        }
                    }
                }
            });

            await Task.Delay(TimeSpan.FromMilliseconds(2 * PollingIntervalShort));
        }

        /// <summary>
        /// How many times <paramref name="value"/> appears in <paramref name="line"/>. Under a burst of parallel
        /// invocations the host's console writes interleave and the newline between two log lines is lost. Then
        /// one physical line can carry two logical ones. Counting occurrences keeps both.
        /// </summary>
        private static int CountOccurrences(string line, string value)
        {
            int occurrences = 0;
            for (int index = line.IndexOf(value, StringComparison.Ordinal); index >= 0; index = line.IndexOf(value, index + value.Length, StringComparison.Ordinal))
            {
                occurrences++;
            }

            return occurrences;
        }

        // The build output and the test project it came from, found from the test assembly rather than the
        // working directory, which each test runner sets differently. func runs from the build output and
        // the harness reads the project's settings files from the project.
        private static readonly string outputDirectory = Path.GetDirectoryName(typeof(IntegrationTestHelpers).Assembly.Location);
        private static readonly string projectDirectory = new DirectoryInfo(outputDirectory).Parent.Parent.Parent.FullName;

        // The connection string the tests and the Functions host share. local.settings.json is the default and
        // REDIS_CONNECTION_STRING overrides it. One committed file serves every machine.
        private static readonly string redisConnectionString = GetRedisConnectionString();

        internal static IConfiguration localsettings = new ConfigurationBuilder()
            .AddJsonFile(GetTestProjectFile("local.settings.json"))
            .AddInMemoryCollection(new Dictionary<string, string> { { ConnectionString, redisConnectionString } })
            .Build();

        internal static IConfiguration hostsettings = new ConfigurationBuilder().AddJsonFile(GetTestProjectFile("host.json")).Build();

        // The port a spawned server listens on, taken from the connection string so the two can never
        // disagree. It is deliberately not 6379. A Redis already running on the developer's machine is
        // never in the way.
        private static readonly int redisPort = GetRedisPort();

        private static string GetTestProjectFile(string fileName)
        {
            return Path.Combine(projectDirectory, fileName);
        }

        private static string GetRedisConnectionString()
        {
            string overrideConnectionString = Environment.GetEnvironmentVariable(RedisConnectionStringVariable);
            if (!string.IsNullOrWhiteSpace(overrideConnectionString))
            {
                return overrideConnectionString;
            }

            IConfiguration file = new ConfigurationBuilder().AddJsonFile(GetTestProjectFile("local.settings.json")).Build();
            return file.GetSection("Values")[ConnectionString];
        }

        private static int GetRedisPort()
        {
            ConfigurationOptions options = ConfigurationOptions.Parse(redisConnectionString);
            switch (options.EndPoints.Single())
            {
                case DnsEndPoint dnsEndPoint:
                    return dnsEndPoint.Port;
                case IPEndPoint ipEndPoint:
                    return ipEndPoint.Port;
                default:
                    throw new InvalidOperationException($"The '{ConnectionString}' connection string does not name a host and port.");
            }
        }

        private static ConfigurationOptions GetListeningRedisOptions()
        {
            ConfigurationOptions options = ConfigurationOptions.Parse(redisConnectionString);
            options.AbortOnConnectFail = true;
            options.ConnectTimeout = 1000;
            options.AllowAdmin = true;
            return options;
        }

        private static Version GetListeningRedisVersion()
        {
            try
            {
                using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(GetListeningRedisOptions()))
                {
                    return multiplexer.GetServers().Single().Version;
                }
            }
            catch (RedisConnectionException)
            {
                return null;
            }
        }

        /// <summary>
        /// When a server is already listening at the configured endpoint, flushes it and turns on the keyspace
        /// notifications the pub/sub tests rely on, which a spawned server gets from its command line.
        /// </summary>
        private static bool TryResetListeningRedis()
        {
            try
            {
                using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(GetListeningRedisOptions()))
                {
                    IServer server = multiplexer.GetServers().Single();
                    server.FlushAllDatabases();
                    server.ConfigSet("notify-keyspace-events", "AKE");
                    return true;
                }
            }
            catch (RedisConnectionException)
            {
                return false;
            }
        }

        private static string GetRedisBuildFileName(string versionPath)
        {
            return $"{versionPath}/src/redis-server";
        }

        /// <summary>
        /// How to spawn a server for the test. The exact build wins when it is present, then the server named
        /// by <c>REDIS_SERVER</c>, then the first <c>redis-server</c> on the PATH. Windows falls back to the
        /// build inside WSL, which is where upstream keeps it.
        /// </summary>
        private static ProcessStartInfo GetRedisServerStartInfo(string versionPath)
        {
            string build = GetRedisBuildFileName(versionPath);
            string arguments = $"--port {redisPort} --notify-keyspace-events AKE";
            bool windows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            string fileName = !windows && File.Exists(build) ? build : ResolveTool(windows ? "redis-server.exe" : "redis-server", RedisServerVariable);
            if (fileName != null)
            {
                return CreateStartInfo(fileName, arguments);
            }

            if (windows)
            {
                return CreateStartInfo(@"C:\Windows\System32\wsl.exe", $"{build} {arguments}");
            }

            throw new FileNotFoundException($"No Redis server is listening at '{redisConnectionString}', 'redis-server' was not found on the PATH and there is no build at '{build}'. Start a server there, install one, add it to the PATH or set {RedisServerVariable} to its location.");
        }

        private static string GetFunctionsFileName()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return GetWindowsFunctionsFilePath();
            }

            string fileName = ResolveTool("func", FunctionsCoreToolsVariable) ?? (File.Exists("/usr/bin/func") ? "/usr/bin/func" : null);
            if (fileName == null)
            {
                throw new FileNotFoundException($"'func' was not found on the PATH or at '/usr/bin/func'. Install Azure Functions Core Tools, add it to the PATH or set {FunctionsCoreToolsVariable} to its location.");
            }

            return fileName;
        }

        private static ProcessStartInfo CreateStartInfo(string fileName, string arguments)
        {
            return new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                WindowStyle = ProcessWindowStyle.Hidden,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
        }

        /// <summary>
        /// The tool named by the environment variable, otherwise the first match on the PATH, otherwise null.
        /// </summary>
        private static string ResolveTool(string fileName, string variable)
        {
            string overridePath = Environment.GetEnvironmentVariable(variable);
            if (!string.IsNullOrWhiteSpace(overridePath))
            {
                if (!File.Exists(overridePath))
                {
                    throw new FileNotFoundException($"{variable} is set to '{overridePath}', which does not exist.");
                }
                return overridePath;
            }

            string[] directories = (Environment.GetEnvironmentVariable("PATH") ?? string.Empty).Split(Path.PathSeparator);
            return directories.Select(directory => Path.Combine(directory, fileName)).FirstOrDefault(File.Exists);
        }

        private static string GetWindowsFunctionsFilePath()
        {
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "cmd.exe",
                    Arguments = "/c where func",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                }
            };

            proc.Start();
            string filepath = proc.StandardOutput.ReadLine();
            proc.WaitForExit();
            return filepath;
        }

        internal static string GetLogValue(object value)
        {
            return value.GetType().FullName + ":" + JsonConvert.SerializeObject(value);
        }

        internal class ClientSecretCredentialComponentFactory : AzureComponentFactory
        {
            public override object CreateClient(Type clientType, IConfiguration configuration, TokenCredential credential, object clientOptions)
            {
                throw new NotImplementedException();
            }

            public override object CreateClientOptions(Type optionsType, object serviceVersion, IConfiguration configuration)
            {
                throw new NotImplementedException();
            }

            public override TokenCredential CreateTokenCredential(IConfiguration configuration)
            {
                var clientId = configuration["clientId"];
                var tenantId = configuration["tenantId"];
                var clientSecret = configuration["clientSecret"];
                return new ClientSecretCredential(tenantId, clientId, clientSecret);
            }
        }
    }
}
