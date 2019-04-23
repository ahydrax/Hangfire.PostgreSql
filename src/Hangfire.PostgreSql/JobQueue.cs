using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal sealed class JobQueue : IJobQueue
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly PostgreSqlStorageOptions _options;

        public JobQueue(IConnectionProvider connectionProvider, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ThrowIfCollectionIsNullOrEmpty(queues, nameof(queues));
            cancellationToken.ThrowIfCancellationRequested();

            var queuesList = string.Join(",", queues.Select(x => $"'{x}'"));
            var queuesOrder = string.Join(",", queues.Select(x => $@"queue='{x}'"));

            var fetchJobSqlTemplate = $@"
UPDATE jobqueue AS jobqueue
SET fetchedat = NOW() AT TIME ZONE 'UTC'
WHERE jobqueue.id = (
    SELECT id
    FROM jobqueue
    WHERE queue IN ({queuesList})
    AND (fetchedat IS NULL OR fetchedat < NOW() AT TIME ZONE 'UTC' - @timeout)
    ORDER BY ({queuesOrder}) DESC
    LIMIT 1
    FOR UPDATE SKIP LOCKED)
RETURNING jobqueue.id AS Id, jobid AS JobId, queue AS Queue, fetchedat AS FetchedAt;
";

            FetchedJobDto fetchedJobDto;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    fetchedJobDto = connectionHolder.Connection.Query<FetchedJobDto>(
                            fetchJobSqlTemplate,
                            new { queues = queues, timeout = _options.InvisibilityTimeout })
                        .SingleOrDefault();
                }

                if (fetchedJobDto == null)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                }

            } while (fetchedJobDto == null);

            return new FetchedJob(
                _connectionProvider,
                fetchedJobDto.Id,
                fetchedJobDto.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJobDto.Queue);
        }

        public void Enqueue(string queue, long jobId)
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string query = @"
INSERT INTO jobqueue (jobid, queue) 
VALUES (@jobId, @queue)
";
                var parameters = new { jobId = jobId, queue = queue };
                connectionHolder.Connection.Execute(query, parameters);
            }
        }
    }
}
