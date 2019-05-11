using System;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Maintenance
{
#pragma warning disable 618 // TODO: remove IServerComponent when migrating to Hangfire 2
    internal sealed class ExpiredLocksManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _lockTimeOut;

        public ExpiredLocksManager(IConnectionProvider connectionProvider, TimeSpan lockTimeOut)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfValueIsNotPositive(lockTimeOut, nameof(lockTimeOut));

            _connectionProvider = connectionProvider;
            _lockTimeOut = lockTimeOut;
        }

        public override string ToString() => "PostgreSQL Expired Locks Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.StoppingToken);

        public void Execute(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            const string query = @"
delete from lock
where acquired < current_timestamp at time zone 'UTC' - @timeout";

            _connectionProvider.Execute(query, new { timeout = _lockTimeOut });

            cancellationToken.Wait(_lockTimeOut);
        }
    }
}
