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
insert into jobqueue (jobid, queue) 
values (@jobId, @queue)
";
            var parameters = new { jobId = jobId, queue = queue };
            _connectionProvider.Execute(query, parameters);
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ThrowIfCollectionIsNullOrEmpty(queues, nameof(queues));
            cancellationToken.ThrowIfCancellationRequested();

            var fetchJobSqlTemplate = $@"
update jobqueue as jobqueue
set fetchedat = @fetched
where jobqueue.id = (
    select id
    from jobqueue
    where queue IN ('{string.Join("', '", queues)}')
    and (fetchedat is null or fetchedat < @timeout)
    order by jobqueue.id desc
    limit 1
    FOR update SKIP LOCKED)
returning jobqueue.id as Id, jobid as JobId, queue as Queue, fetchedat as FetchedAt;
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
