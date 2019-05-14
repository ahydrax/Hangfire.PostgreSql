using System;
using System.Data;
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Maintenance
{
#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
    internal sealed class ExpirationManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly string[] ProcessedTables =
        {
            "counter",
            "job",
            "list",
            "set",
            "hash",
        };

        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(IConnectionProvider connectionProvider)
            : this(connectionProvider, TimeSpan.FromHours(1))
        {
        }

        public ExpirationManager(IConnectionProvider connectionProvider, TimeSpan checkInterval)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfValueIsNotPositive(checkInterval, nameof(checkInterval));

            _connectionProvider = connectionProvider;
            _checkInterval = checkInterval;
        }

        public override string ToString() => "PostgreSQL Expiration Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.StoppingToken);

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                var query = $@"delete from {table} where expireat is not null and expireat < now() at time zone 'UTC';";

                var removedCount = _connectionProvider.Execute(query);
                if (removedCount > 0)
                {
                    Logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);
                }

                cancellationToken.Wait(_checkInterval);
            }
        }
    }
}
