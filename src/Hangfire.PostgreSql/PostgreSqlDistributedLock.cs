using System;
using System.Diagnostics;
using System.Threading;
using Dapper;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlDistributedLock : IDisposable
    {
        private static readonly ThreadLocal<Random> Random = new ThreadLocal<Random>(() => new Random());

        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private bool _completed;

        public PostgreSqlDistributedLock(string resource,
            TimeSpan timeout,
            IPostgreSqlConnectionProvider connectionProvider)
        {
            Guard.ThrowIfNullOrEmpty(resource, nameof(resource));
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));

            _resource = resource;
            _timeout = timeout;
            _connectionProvider = connectionProvider;

            Initialize();
        }

        private void Initialize()
        {
            var sleepTime = 50;
            var lockAcquiringWatch = Stopwatch.StartNew();
            do
            {
                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    const string query = @"
INSERT INTO lock(resource, acquired) 
VALUES (@resource, current_timestamp at time zone 'UTC')
ON CONFLICT (resource) DO NOTHING
;";
                    var parameters = new { resource = _resource };
                    var rowsAffected = connectionHolder.Connection.Execute(query, parameters);

                    if (rowsAffected > 0) return;
                }
            } while (IsNotTimeouted(lockAcquiringWatch.Elapsed, ref sleepTime));

            throw new PostgreSqlDistributedLockException(
                $"Could not place a lock on the resource \'{_resource}\': Lock timeout.");
        }

        private bool IsNotTimeouted(TimeSpan elapsed, ref int sleepTime)
        {
            if (elapsed > _timeout)
            {
                return false;
            }
            else
            {
                Thread.Sleep(sleepTime);

                var maxSleepTimeMilliseconds = Math.Min(sleepTime * 2, 2000);
                sleepTime = Random.Value.Next(sleepTime, maxSleepTimeMilliseconds);

                return true;
            }
        }

        public void Dispose()
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                if (_completed) return;
                _completed = true;

                const string query = @"
DELETE FROM lock 
WHERE resource = @resource;
";
                var rowsAffected = connectionHolder.Connection.Execute(query, new { resource = _resource });
                if (rowsAffected <= 0)
                {
                    throw new PostgreSqlDistributedLockException(
                        $"Could not release a lock on the resource '{_resource}'. Lock does not exists.");
                }
            }
        }
    }
}
