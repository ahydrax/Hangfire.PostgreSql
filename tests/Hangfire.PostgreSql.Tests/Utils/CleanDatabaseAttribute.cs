using System.Reflection;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
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
            var schemaName = ConnectionUtils.GetSchemaName();
            var provider = ConnectionUtils.GetConnectionProvider();
            var databaseName = ConnectionUtils.GetDatabaseName();
            var databaseExists = provider.FetchScalar<int>(
                                     @"select count(*) from pg_database where datname = @databaseName;",
                                     new { databaseName = databaseName }) > 0;

            if (!databaseExists)
            {
                provider.Execute(@"create database @databaseName", new { databaseName = databaseName });
            }

            new DatabaseInitializer(provider, schemaName).Initialize();
        }

        private static void CleanTables()
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            const string cleanQuery = @"
truncate counter cascade;
truncate hash cascade;
truncate job cascade;
truncate jobparameter cascade;
truncate jobqueue cascade;
truncate list cascade;
truncate lock cascade;
truncate server cascade;
truncate set cascade;
truncate state cascade;
";

            provider.Execute(cleanQuery);
        }
    }
}
