using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using static Microsoft.AspNetCore.WebHost;

namespace Hangfire.PostgreSql.Tests.Web
{
    public static class Program
    {
        public static void Main(string[] args) => 
            CreateDefaultBuilder(args)
                .UseStartup<Startup>()
                .ConfigureLogging(loggingBuilder => 
                    loggingBuilder.AddFilter<ConsoleLoggerProvider>(level => 
                        level == LogLevel.None))
                .Build()
                .Run();
    }
}
