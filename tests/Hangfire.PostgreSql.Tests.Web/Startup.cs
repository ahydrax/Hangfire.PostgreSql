using System;
using Hangfire.Console;
using Hangfire.PostgreSql.Tests.Integration;
using Hangfire.PostgreSql.Tests.Utils;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hangfire.PostgreSql.Tests.Web
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            var connectionString = Environment.GetEnvironmentVariable("Hangfire_PostgreSql_ConnectionString");
            services.AddMvc();
            services.AddHangfire(configuration =>
            {
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ActiveConnections);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.MaxConnections);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ConnectionUsageRatio);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.CacheHitsPerRead);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.DistributedLocksCount);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlLocksCount);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlServerVersion);
                configuration.UsePostgreSqlStorage(connectionString);
                configuration.UseConsole();
            });
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.UseDeveloperExceptionPage();
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                ServerCheckInterval = TimeSpan.FromSeconds(15),
                HeartbeatInterval = TimeSpan.FromSeconds(5),
                ServerTimeout = TimeSpan.FromSeconds(15),
                ServerName = "Hangfire Test Server",
                WorkerCount = 50,
                Queues = new[] { "queue2", "queue1", "default" }
            });

            app.UseHangfireDashboard("", new DashboardOptions { StatsPollingInterval = 1000 });
            RecurringJob.AddOrUpdate(() => TestSuite.Alloc(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => TestSuite.CpuKill(25), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => TestSuite.ContinuationTest(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => TestSuite.TaskBurst(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => DistributedLockTest.Ctor_ActuallyGrantsExclusiveLock(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => DistributedLockTest.Perf_AcquiringLock_DifferentResources(), Cron.Yearly, TimeZoneInfo.Utc);
            RecurringJob.AddOrUpdate(() => DistributedLockTest.Perf_AcquiringLock_SameResource(), Cron.Yearly, TimeZoneInfo.Utc);
        }
    }
}
