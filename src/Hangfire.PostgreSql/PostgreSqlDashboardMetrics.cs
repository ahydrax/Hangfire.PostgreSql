using System;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Npgsql;

namespace Hangfire.PostgreSql
{
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

        [PublicAPI]
        public static readonly DashboardMetric MaxConnections = new DashboardMetric(
            "pg:connections:max",
            "Max connections",
            page => GetMetricByQuery<long>(page, @"SHOW max_connections;"));

        [PublicAPI]
        public static readonly DashboardMetric ActiveConnections = new DashboardMetric(
            "pg:connections:active",
            "Active connections",
            page => GetMetricByQuery<long>(page, @"SELECT numbackends from pg_stat_database WHERE datname = current_database();"));

        [PublicAPI]
        public static readonly DashboardMetric PostgreSqlLocksCount = new DashboardMetric(
            "pg:locks:count",
            "PostgreSQL locks",
            page => GetMetricByQuery<long>(page, @"SELECT COUNT(*) FROM pg_locks;"));

        [PublicAPI]
        public static readonly DashboardMetric DistributedLocksCount = new DashboardMetric(
            "app:locks:count",
            "Application locks",
            page => GetMetricByQuery<long>(page, @"SELECT COUNT(*) FROM lock;"));

        [PublicAPI]
        public static readonly DashboardMetric PostgreSqlServerVersion = new DashboardMetric(
            "pg:version",
            "PostgreSQL version",
            page => Execute(page, x => new Metric(x.PostgreSqlVersion.ToString()), UndefinedMetric));

        [PublicAPI]
        public static readonly DashboardMetric CacheHitsPerRead = new DashboardMetric(
            "pg:cache:hitratio",
            "Cache Hits Per Read",
            page => GetMetricByQuery<long>(page, @"SELECT ROUND(SUM(blks_hit) / SUM(blks_read)) FROM pg_stat_database;"));

        [PublicAPI]
        public static readonly DashboardMetric ConnectionUsageRatio = new DashboardMetric(
            "pg:connections:ratio",
            "Connections used",
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
