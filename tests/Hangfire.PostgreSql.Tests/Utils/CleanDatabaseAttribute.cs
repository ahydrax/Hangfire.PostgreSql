using System.Reflection;
using System.Threading;
using Dapper;
using Xunit;
using Xunit.Sdk;

[assembly: CollectionBehavior(DisableTestParallelization = true)]
namespace Hangfire.PostgreSql.Tests.Utils
{
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();
        private static bool _sqlObjectInstalled;

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);

            if (!_sqlObjectInstalled)
            {
                RecreateSchemaAndInstallObjects();
                _sqlObjectInstalled = true;
            }
            CleanTables();
        }

        public override void After(MethodInfo methodUnderTest)
        {
            try
            {
            }
            finally
            {
                Monitor.Exit(GlobalLock);
            }
        }

        private static void RecreateSchemaAndInstallObjects()
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connectionHolder = provider.AcquireConnection())
            {
                var connection = connectionHolder.Connection;
                var databaseName = ConnectionUtils.GetDatabaseName();
                var databaseExists = connection.ExecuteScalar<int>(@"select count(*) from pg_database where datname = @databaseName;",
                                         new { databaseName = databaseName }
                                     ) > 0;

                if (!databaseExists)
                {
                    connection.Execute(@"CREATE DATABASE @databaseName", new { databaseName = databaseName });
                }
            }

            new DatabaseInitializer(ConnectionUtils.GetConnectionProvider(), ConnectionUtils.GetSchemaName()).Initialize();

            using (var connectionHolder = provider.AcquireConnection())
            {
                PostgreSqlTestObjectsInitializer.CleanTables(connectionHolder.Connection);
            }
        }

        private static void CleanTables()
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = provider.AcquireConnection())
            {
                PostgreSqlTestObjectsInitializer.CleanTables(connection.Connection);
            }
        }
    }
}
