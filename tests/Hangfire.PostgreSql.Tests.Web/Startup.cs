using System;
using Hangfire.PostgreSql.Kafka;
using Hangfire.PostgreSql.Tests.Integration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hangfire.PostgreSql.Tests.Web
{
    public class Startup
    {
        private const string ConnectionStringVariableName = "Hangfire_PostgreSql_ConnectionString";

        private const string DefaultConnectionString =
            @"Server=localhost;Port=5432;Database=hangfire_tests;User Id=postgres;Password=password;Search Path=hangfire";

        public static string GetConnectionString() => Environment.GetEnvironmentVariable(ConnectionStringVariableName) ?? DefaultConnectionString;

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();
            services.AddHangfire(configuration =>
            {
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ActiveConnections);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.CacheHitsPerRead);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ConnectionUsageRatio);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.DistributedLocksCount);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.MaxConnections);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlLocksCount);
                configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlServerVersion);

                //var kafkaQueue = new KafkaJobQueue();
                //configuration.UseStorage(new PostgreSqlStorage(GetConnectionString(), kafkaQueue,
                //    new PostgreSqlStorageOptions()));
                configuration.UsePostgreSqlStorage(GetConnectionString());
            });
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            app.UseDeveloperExceptionPage();
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                ServerCheckInterval = TimeSpan.FromSeconds(2),
                HeartbeatInterval = TimeSpan.FromMilliseconds(500),
                ServerTimeout = TimeSpan.FromSeconds(2),
                ServerName = "Server default"
            });
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                ServerCheckInterval = TimeSpan.FromSeconds(2),
                HeartbeatInterval = TimeSpan.FromMilliseconds(500),
                ServerTimeout = TimeSpan.FromSeconds(2),
                ServerName = "Server Q1",
                Queues = new[] { "queue1" }
            });
            app.UseHangfireServer(new BackgroundJobServerOptions
            {
                ServerCheckInterval = TimeSpan.FromSeconds(2),
                HeartbeatInterval = TimeSpan.FromMilliseconds(500),
                ServerTimeout = TimeSpan.FromSeconds(2),
                ServerName = "Server Q2",
                Queues = new[] { "queue2" }
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
