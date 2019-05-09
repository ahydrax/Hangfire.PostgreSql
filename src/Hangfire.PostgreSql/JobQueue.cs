using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
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

        public void Enqueue(string queue, long jobId)
        {
            const string query = @"
INSERT INTO jobqueue (jobid, queue) 
VALUES (@jobId, @queue)
";
            var parameters = new { jobId = jobId, queue = queue };
            _connectionProvider.Execute(query, parameters);
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ThrowIfCollectionIsNullOrEmpty(queues, nameof(queues));
            cancellationToken.ThrowIfCancellationRequested();

            var fetchJobSqlTemplate = $@"
UPDATE jobqueue AS jobqueue
SET fetchedat = @fetched
WHERE jobqueue.id = (
    SELECT id
    FROM jobqueue
    WHERE queue IN ('{string.Join("', '", queues)}')
    AND (fetchedat IS NULL OR fetchedat < @timeout)
    ORDER BY jobqueue.id DESC
    LIMIT 1
    FOR UPDATE SKIP LOCKED)
RETURNING jobqueue.id AS Id, jobid AS JobId, queue AS Queue, fetchedat AS FetchedAt;
";

            FetchedJobDto fetchedJobDto;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var now = DateTime.UtcNow;
                var parameters = new { fetched = now, timeout = now - _options.InvisibilityTimeout, queues = queues };
                fetchedJobDto = _connectionProvider.FetchFirstOrDefault<FetchedJobDto>(fetchJobSqlTemplate, parameters);

                if (fetchedJobDto == null) cancellationToken.Wait(_options.QueuePollInterval);

            } while (fetchedJobDto == null);

            return new FetchedJob(
                _connectionProvider,
                fetchedJobDto.Id,
                fetchedJobDto.JobId.ToString(CultureInfo.InvariantCulture),
                fetchedJobDto.Queue);
        }
    }
}
