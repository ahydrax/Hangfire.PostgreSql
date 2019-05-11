using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    internal class FetchedJob : IFetchedJob
    {
        private readonly IConnectionProvider _connectionProvider;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public FetchedJob(
            IConnectionProvider connectionProvider,
            long id,
            string jobId,
            string queue)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(jobId, nameof(jobId));
            Guard.ThrowIfNull(queue, nameof(queue));

            _connectionProvider = connectionProvider;
            Id = id;
            JobId = jobId;
            Queue = queue;
        }

        public long Id { get; }
        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            const string query = @"
delete from jobqueue
where id = @id;
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(query, new { id = Id });
                _removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            const string query = @"
update jobqueue 
set fetchedat = null 
where id = @id;
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(query, new { id = Id });
                _requeued = true;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }
    }
}
