using System;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ExpiredLockManagerFacts
    {
        [Fact, CleanDatabase]
        public void Execute_RemovesOnlyExpiredLocks()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var timeout = TimeSpan.FromSeconds(5);
                connection.Execute(@"insert into lock(resource, acquired) values ('fresh_lock', now() at time zone 'UTC')");
                connection.Execute(@"insert into lock(resource, acquired) values ('expired_lock', now() at time zone 'UTC' - @timeout)", new { timeout });

                // Act
                var expiredLocksManager = new ExpiredLocksManager(provider, timeout - TimeSpan.FromSeconds(1));
                expiredLocksManager.Execute(CancellationToken.None);

                // Assert
                var locksCount = connection.ExecuteScalar<int>(@"select count(*) from lock");
                Assert.Equal(1, locksCount);
            });
        }

        private void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }
    }
}
