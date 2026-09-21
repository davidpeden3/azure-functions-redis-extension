using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Hosting;

namespace Microsoft.Azure.Functions.Worker.Extensions.Redis.Tests.Functions
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder(args);
            builder.Build().Run();
        }
    }
}
