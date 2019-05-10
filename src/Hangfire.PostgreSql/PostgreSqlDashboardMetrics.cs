using System;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Npgsql;

namespace Hangfire.PostgreSql
{
    /// <summary>
    /// Contains dashboard metrics
    /// </summary>
    public static class PostgreSqlDashboardMetrics
    {
        /// <summary>
        /// Tells bootstrapper to add the following metrics to dashboard:
        /// * Max connections count
        /// * Active connections count
        /// * PostgreSql server version
        /// </summary>
        /// <param name="configuration">Hangfire configuration</param>
        /// <returns></returns>
        [PublicAPI]
        public static IGlobalConfiguration UsePostgreSqlMetrics(this IGlobalConfiguration configuration)
        {
            configuration.UseDashboardMetric(MaxConnections);
            configuration.UseDashboardMetric(ActiveConnections);
            configuration.UseDashboardMetric(PostgreSqlServerVersion);
            return configuration;
        }

        /// <summary>
        /// Displays maximum available connections.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric MaxConnections = new DashboardMetric(
            "pg:connections:max",
            "Max Connections",
            page => GetMetricByQuery<long>(page, @"SHOW max_connections;"));
        
        /// <summary>
        /// Displays active connections.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric ActiveConnections = new DashboardMetric(
            "pg:connections:active",
            "Connections",
            page => GetMetricByQuery<long>(page, @"SELECT numbackends from pg_stat_database WHERE datname = current_database();"));
        
        /// <summary>
        /// Displays active PostgreSQL locks.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric PostgreSqlLocksCount = new DashboardMetric(
            "pg:locks:count",
            "PostgreSQL Locks",
            page => GetMetricByQuery<long>(page, @"SELECT COUNT(*) FROM pg_locks;"));
        
        /// <summary>
        /// Displays active distributed locks acquired by storage.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric DistributedLocksCount = new DashboardMetric(
            "app:locks:count",
            "Application Locks",
            page => GetMetricByQuery<long>(page, @"SELECT COUNT(*) FROM lock;"));
        
        /// <summary>
        /// Displays PostgreSQL server version.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric PostgreSqlServerVersion = new DashboardMetric(
            "pg:version",
            "PostgreSQL Version",
            page => Execute(page, x => new Metric(x.PostgreSqlVersion.ToString()), UndefinedMetric));
        
        /// <summary>
        /// Displays PostgreSQL server cache hit ratio.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric CacheHitsPerRead = new DashboardMetric(
            "pg:cache:hitratio",
            "Cache Hit Ratio",
            page => GetMetricByQuery<long>(page, @"SELECT ROUND(SUM(blks_hit) / SUM(blks_read)) FROM pg_stat_database;"));
        
        /// <summary>
        /// Displays PostgreSQL connection usage ratio.
        /// </summary>
        [PublicAPI]
        public static readonly DashboardMetric ConnectionUsageRatio = new DashboardMetric(
            "pg:connections:ratio",
            "Connections Used",
            page => Execute(page, connection =>
            {
                var max = connection.ExecuteScalar<long>(@"SHOW max_connections;");
                var current = connection.ExecuteScalar<long>(@"SELECT numbackends from pg_stat_database WHERE datname = current_database();");

                var ratio = current * 100 / max;
                var ratioString = ratio + "%";
                var metric = new Metric(ratioString);
                if (ratio < 50)
                {
                    metric.Style = MetricStyle.Success;
                }
                else if (ratio < 90)
                {
                    metric.Style = MetricStyle.Warning;
                }
                else
                {
                    metric.Style = MetricStyle.Danger;
                }
                return metric;
            }, UndefinedMetric)
        );

        private static readonly Metric UndefinedMetric = new Metric("N/A") { Highlighted = true, Style = MetricStyle.Danger };

        private static Metric GetMetricByQuery<T>(RazorPage page, string query)
        {
            return Execute(page, connection =>
            {
                var metric = connection.ExecuteScalar<T>(query);
                return new Metric(metric.ToString());
            }, UndefinedMetric);
        }

        private static T Execute<T>(RazorPage page, Func<NpgsqlConnection, T> func, T fallbackValue)
        {
            if (page.Storage is PostgreSqlStorage storage)
            {
                using (var connectionHolder = storage.ConnectionProvider.AcquireConnection())
                {
                    return func(connectionHolder.Connection);
                }
            }
            else
            {
                return fallbackValue;
            }
        }
    }
}
