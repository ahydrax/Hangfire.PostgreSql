using System;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Maintenance
{
#pragma warning disable 618 // TODO: remove IServerComponent when migrating to Hangfire 2
    public class ExpiredLocksManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _lockTimeOut;

        internal ExpiredLocksManager(IConnectionProvider connectionProvider, TimeSpan lockTimeOut)
        {
            _connectionProvider = connectionProvider;
            _lockTimeOut = lockTimeOut;
        }

        public void Execute(BackgroundProcessContext context) => Execute(context.StoppingToken);

        public void Execute(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string query = @"
DELETE FROM lock
WHERE acquired < current_timestamp at time zone 'UTC' - @timeout";

                var parameters = new
                {
                    timeout = _lockTimeOut
                };
                connectionHolder.Connection.Execute(query, parameters);
            }

            cancellationToken.WaitHandle.WaitOne(_lockTimeOut);
        }
    }
}
