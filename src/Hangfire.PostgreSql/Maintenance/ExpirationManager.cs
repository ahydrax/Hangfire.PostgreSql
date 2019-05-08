using System;
using System.Data;
using System.Globalization;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Maintenance
{
#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
    internal sealed class ExpirationManager : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

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
            _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
            _checkInterval = checkInterval;
        }

        public override string ToString() => "PostgreSql Expiration Manager";

        public void Execute(BackgroundProcessContext context) => Execute(context.CancellationToken);

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount;
                do
                {
                    using (var connectionHolder = _connectionProvider.AcquireConnection())
                    using (var transactionHolder = connectionHolder.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        // Pgsql doesn't support parameters for table names that's why you're going this 'awful' sql query interpolation
                        var query = $@"
DELETE FROM {table}
WHERE id IN (
    SELECT id
    FROM {table}
    WHERE expireat < NOW() AT TIME ZONE 'UTC'
    LIMIT {Convert.ToString(NumberOfRecordsInSinglePass, CultureInfo.InvariantCulture)}
)";
                        removedCount = connectionHolder.Connection.Execute(query, transaction: transactionHolder.Transaction);
                        transactionHolder.Commit();
                    }

                    if (removedCount > 0)
                    {
                        Logger.InfoFormat("Removed {0} outdated record(s) from '{1}' table.", removedCount, table);

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount != 0);
            }
            cancellationToken.WaitHandle.WaitOne(_checkInterval);
            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}
