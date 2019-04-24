using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;
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

                var method = GetType().GetMethod(nameof(TestPage.Assign), BindingFlags.NonPublic | BindingFlags.Instance);

                var context = new TestContext(storage, new DashboardOptions());
                method.Invoke(this, new object[] { context });
            }

            public override void Execute() { }
        }

        private class TestContext : DashboardContext
        {
            public TestContext([NotNull] JobStorage storage, [NotNull] DashboardOptions options)
                : base(storage, options)
            {

            }
        }
    }
}
