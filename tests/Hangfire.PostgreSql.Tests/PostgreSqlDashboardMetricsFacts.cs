using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Dashboard;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlDashboardMetricsFacts
    {
        [Theory]
        [MemberData(nameof(GetMetrics))]
        public void DashboardMetric_Returns_Value(DashboardMetric dashboardMetric)
        {

            var page = new TestPage();

            var metric = dashboardMetric.Func(page);

            Assert.NotEqual("???", metric.Value);
        }

        // ReSharper disable once MemberCanBePrivate.Global
        public static IEnumerable<object[]> GetMetrics()
        {
            yield return new object[] { PostgreSqlDashboardMetrics.MaxConnections };
            yield return new object[] { PostgreSqlDashboardMetrics.DistributedLocksCount };
            yield return new object[] { PostgreSqlDashboardMetrics.ActiveConnections };
            yield return new object[] { PostgreSqlDashboardMetrics.CacheHitsPerRead };
            yield return new object[] { PostgreSqlDashboardMetrics.PostgreSqlLocksCount };
            yield return new object[] { PostgreSqlDashboardMetrics.PostgreSqlServerVersion };
        }

        private class TestPage : RazorPage
        {
            public TestPage()
            {
                var connectionString = ConnectionUtils.GetConnectionString();
                var storage = new PostgreSqlStorage(connectionString, new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = false });

                var context = (DashboardContext)GetType().GetProperty("Context", typeof(DashboardContext)).GetValue(this);

                typeof(DashboardContext).GetProperty("Storage", typeof(JobStorage)).SetValue(context, storage);
            }

            public override void Execute() { }
        }
    }
}
