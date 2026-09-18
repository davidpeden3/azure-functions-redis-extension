using Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions;
using Newtonsoft.Json.Linq;
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

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Integration
{
    internal static class IntegrationTestHelpers
    {
        // Environment variables that override where the harness finds its tools. Set them when the tool is
        // installed somewhere the test runner's PATH does not reach, which is common under an IDE.
        internal const string FunctionsCoreToolsVariable = "AZURE_FUNCTIONS_CORE_TOOLS";
        internal const string RedisServerVariable = "REDIS_SERVER";

        // Overrides the Redis connection string in the app's local.settings.json for both the tests and the
        // Functions host they start. Point it at a server that is already running, such as a dedicated test
        // instance on another machine. The harness then uses that server instead of spawning one. The server is
        // flushed before every test. It must be dedicated to these tests.
        internal const string RedisConnectionStringVariable = "REDIS_CONNECTION_STRING";

        // Where the harness writes each Functions host's full output, relative to the build output.
        internal const string HostLogDirectory = "func-logs";
        private static int hostLogSequence;

        // The isolated app's build output, where func runs from and where the app's settings files are. The app
        // builds in the Functions folder beside this project for the same configuration and framework. Its output
        // therefore sits at the same path relative to its project directory as this assembly does to this project's.
        // The folders are short because the Functions SDK nests a generated project under the app's obj and
        // Visual Studio's MSBuild still stops at 260 characters.
        private static readonly string outputDirectory = Path.GetDirectoryName(typeof(IntegrationTestHelpers).Assembly.Location);
        private static readonly string projectDirectory = new DirectoryInfo(outputDirectory).Parent.Parent.Parent.FullName;
        internal static readonly string functionsDirectory = Path.GetFullPath(Path.Combine(projectDirectory, "..", "Functions", Path.GetRelativePath(projectDirectory, outputDirectory)));

        // The connection string the tests and the Functions host share. The app's local.settings.json is the
        // default and REDIS_CONNECTION_STRING overrides it. One committed file serves every machine.
        internal static readonly string redisConnectionString = GetRedisConnectionString();

        // The port a spawned server listens on, taken from the connection string so the two can never
        // disagree. It is deliberately not 6379. A Redis already running on the developer's machine is
        // never in the way.
        private static readonly int redisPort = GetRedisPort();

        internal static string GetFunctionsFile(string fileName)
        {
            return Path.Combine(functionsDirectory, fileName);
        }

        /// <summary>
        /// The name the host gives a worker's function: the function's own name under the Functions namespace.
        /// The host logs invocations under it and the extension names the Redis connection it opens for the
        /// function's trigger after it.
        /// </summary>
        internal static string GetHostFunctionName(string functionName)
        {
            return $"Functions.{functionName}";
        }

        /// <summary>
        /// The line the host logs when an invocation of the function completes.
        /// </summary>
        internal static string GetExecutedLogValue(string functionName)
        {
            return $"Executed '{GetHostFunctionName(functionName)}' (Succeeded";
        }

        /// <summary>
        /// Starts a Functions host that serves only <paramref name="functionName"/> and counts down
        /// <paramref name="counts"/> as the host's output arrives. The counter is attached before the host starts
        /// so that no line is missed, because a trigger over data that is already in Redis fires as soon as its
        /// listener starts.
        /// </summary>
        internal static async Task<Process> StartFunctionAsync(string functionName, int port, IDictionary<string, int> counts)
        {
            // func runs from the app's build output, which holds the worker, its metadata and the extension
            // bundle the build produced. The worker is a child process of func. Every kill takes the process tree.
            ProcessStartInfo info = new ProcessStartInfo
            {
                FileName = GetFunctionsFileName(),
                Arguments = $"start --verbose --functions {functionName} --port {port} --no-build",
                WindowStyle = ProcessWindowStyle.Hidden,
                WorkingDirectory = functionsDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            // Core Tools injects local.settings.json into the host's environment under this prefix and leaves
            // any variable that already exists alone. This keeps the host on the same server as the test.
            info.EnvironmentVariables[$"ConnectionStrings:{TestFunctionHelpers.ConnectionString}"] = redisConnectionString;
            Process functionsProcess = new Process() { StartInfo = info };
            functionsProcess.OutputDataReceived += CounterHandlerCreator(counts);

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
                if (e.Data?.Contains("Host started") ?? false)
                {
                    hostStarted.TrySetResult(true);
                }
            }
            functionsProcess.OutputDataReceived += hostStartupHandler;

            TaskCompletionSource<bool> functionLoaded = new TaskCompletionSource<bool>();
            void functionLoadedHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data?.Contains("Generating 1 job function(s)") ?? false)
                {
                    functionLoaded.TrySetResult(true);
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

            // The extension names the connection it opens for a trigger after the function. Its absence means the
            // listener never started, whatever the host logged.
            IConnectionMultiplexer multiplexer = await ConnectionMultiplexer.ConnectAsync(GetListeningRedisOptions());
            ClientInfo[] clients = multiplexer.GetServers()[0].ClientList();
            if (!clients.Any(client => client.Name == $"AzureFunctionsRedisExtension.{GetHostFunctionName(functionName)}"))
            {
                functionsProcess.Kill(entireProcessTree: true);
                throw new Exception($"Function client not found on redis server. Connected clients: {string.Join(", ", clients.Select(client => client.Name))}");
            }

            return functionsProcess;
        }

        /// <summary>
        /// Gives the test an empty Redis server at the configured endpoint. When a server is already listening
        /// there it is flushed and reused and nothing is returned to stop. Otherwise one is spawned for the test.
        /// </summary>
        internal static Process StartRedis()
        {
            if (TryResetListeningRedis())
            {
                return null;
            }

            Process redisProcess = new Process() { StartInfo = GetRedisServerStartInfo() };

            TaskCompletionSource<bool> serverStarted = new TaskCompletionSource<bool>();
            void serverStartupHandler(object sender, DataReceivedEventArgs e)
            {
                if (e.Data?.Contains("* Ready to accept connections") ?? false)
                {
                    serverStarted.TrySetResult(true);
                }
            }
            redisProcess.OutputDataReceived += serverStartupHandler;

            redisProcess.Start();
            redisProcess.BeginOutputReadLine();
            redisProcess.BeginErrorReadLine();
            if (!serverStarted.Task.Wait(TimeSpan.FromMinutes(1)))
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

            redis.Kill(entireProcessTree: true);
        }

        internal static DataReceivedEventHandler CounterHandlerCreator(IDictionary<string, int> counts)
        {
            return (object sender, DataReceivedEventArgs e) =>
            {
                if (e.Data == null)
                {
                    return;
                }

                // Output arrives on several threads at once. The decrement is a read and a write. Without the
                // lock decrements are lost and a count ends above zero.
                lock (counts)
                {
                    foreach (string key in counts.Keys.ToList())
                    {
                        counts[key] -= CountOccurrences(e.Data, key);
                    }
                }
            };
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

        private static string GetRedisConnectionString()
        {
            string overrideConnectionString = Environment.GetEnvironmentVariable(RedisConnectionStringVariable);
            if (!string.IsNullOrWhiteSpace(overrideConnectionString))
            {
                return overrideConnectionString;
            }

            JObject localSettings = JObject.Parse(File.ReadAllText(GetFunctionsFile("local.settings.json")));
            return (string)localSettings["ConnectionStrings"][TestFunctionHelpers.ConnectionString];
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
                    throw new InvalidOperationException($"The '{TestFunctionHelpers.ConnectionString}' connection string does not name a host and port.");
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

        /// <summary>
        /// When a server is already listening at the configured endpoint, flushes it so the test starts empty.
        /// </summary>
        private static bool TryResetListeningRedis()
        {
            try
            {
                using (ConnectionMultiplexer multiplexer = ConnectionMultiplexer.Connect(GetListeningRedisOptions()))
                {
                    multiplexer.GetServers().Single().FlushAllDatabases();
                    return true;
                }
            }
            catch (RedisConnectionException)
            {
                return false;
            }
        }

        /// <summary>
        /// How to spawn a server for the test: the server named by <c>REDIS_SERVER</c>, otherwise the first
        /// <c>redis-server</c> on the PATH.
        /// </summary>
        private static ProcessStartInfo GetRedisServerStartInfo()
        {
            bool windows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            string fileName = ResolveTool(windows ? "redis-server.exe" : "redis-server", RedisServerVariable);
            if (fileName == null)
            {
                throw new FileNotFoundException($"No Redis server is listening at '{redisConnectionString}' and 'redis-server' was not found on the PATH. Start a server there, install one, add it to the PATH or set {RedisServerVariable} to its location.");
            }

            return CreateStartInfo(fileName, $"--port {redisPort}");
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
    }
}
