using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using FetchedJobDto = Hangfire.Storage.Monitoring.FetchedJobDto;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal sealed class MonitoringApi : IMonitoringApi
    {
        private const string AscOrder = "asc";
        private const string DescOrder = "desc";

        private readonly IConnectionProvider _connectionProvider;

        public MonitoringApi(IConnectionProvider connectionProvider)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));

            _connectionProvider = connectionProvider;
        }

        public long ScheduledCount()
            => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long EnqueuedCount(string queue)
        {
            const string query = @"
select count(*) 
from jobqueue 
where queue = @queue 
and fetchedat is null
";
            return GetLong(queue, query);
        }

        public long FetchedCount(string queue)
        {
            const string query = @"
select count(*) 
from jobqueue 
where queue = @queue
and fetchedat is not null
";
            return GetLong(queue, query);
        }

        private long GetLong(string queue, string query)
            => _connectionProvider.FetchScalar<long>(query, new { queue = queue });

        public long FailedCount()
            => GetNumberOfJobsByStateName(FailedState.StateName);

        public long ProcessingCount()
            => GetNumberOfJobsByStateName(ProcessingState.StateName);

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
            => GetJobs(from, count,
                   ProcessingState.StateName,
                   (sqlJob, job, stateData) => new ProcessingJobDto
                   {
                       Job = job,
                       ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                       StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"]),
                   });

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
            => GetJobs(from, count,
                   ScheduledState.StateName,
                   (sqlJob, job, stateData) => new ScheduledJobDto
                   {
                       Job = job,
                       EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                       ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
                   });

        public IDictionary<DateTime, long> SucceededByDatesCount()
            => GetTimelineStats(SucceededState.StateName);

        public IDictionary<DateTime, long> FailedByDatesCount()
            => GetTimelineStats(FailedState.StateName);

        public IList<ServerDto> Servers()
        {
            List<Entities.Server> serverDtos;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string query = @"select * from server";
                serverDtos = connectionHolder.Connection.Query<Entities.Server>(query).ToList();
            }

            var servers = new List<ServerDto>(serverDtos.Count);
            foreach (var server in serverDtos)
            {
                var data = SerializationHelper.Deserialize<ServerData>(server.Data);
                servers.Add(new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.LastHeartbeat,
                    Queues = data.Queues,
                    StartedAt = data.StartedAt,
                    WorkersCount = data.WorkerCount
                });
            }

            return servers;
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
            => GetJobs(from,
                   count,
                   FailedState.StateName,
                   (sqlJob, job, stateData) => new FailedJobDto
                   {
                       Job = job,
                       Reason = sqlJob.StateReason,
                       ExceptionDetails = stateData["ExceptionDetails"],
                       ExceptionMessage = stateData["ExceptionMessage"],
                       ExceptionType = stateData["ExceptionType"],
                       FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                   }, DescOrder);

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
            => GetJobs(from,
                   count,
                   SucceededState.StateName,
                   (sqlJob, job, stateData) => new SucceededJobDto
                   {
                       Job = job,
                       Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
                       TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                           ? new long?(long.Parse(stateData["PerformanceDuration"]) + long.Parse(stateData["Latency"]))
                           : null,
                       SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                   }, DescOrder);

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
            => GetJobs(from,
                   count,
                   DeletedState.StateName,
                   (sqlJob, job, stateData) => new DeletedJobDto
                   {
                       Job = job,
                       DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                   }, DescOrder);

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var queues = GetQueues().ToArray();

            var queueInfos = new List<QueueWithTopEnqueuedJobsDto>(queues.Length);
            foreach (var queue in queues)
            {
                var counters = GetEnqueuedAndFetchedCount(queue);
                var firstJobs = EnqueuedJobs(queue, 0, 5);

                queueInfos.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue,
                    Length = counters.Enqueued,
                    Fetched = counters.Fetched,
                    FirstJobs = firstJobs
                });
            }

            return queueInfos;
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
            => GetHourlyTimelineStats(SucceededState.StateName);

        public IDictionary<DateTime, long> HourlyFailedJobs()
            => GetHourlyTimelineStats(FailedState.StateName);

        public JobDetailsDto JobDetails(string jobId)
        {
            const string jobSql = @"
select id as Id, 
       invocationdata as InvocationData, 
       arguments as Arguments,
       createdat as CreatedAt,
       expireat as ExpireAt
from job
where id = @id;";

            const string parametersSql = @"
select jobid as JobId, 
       name as Name,
       value as Value
from jobparameter 
where jobid = @id;";

            const string stateSql = @"
select jobid as JobId, 
       name as Name, 
       reason as Reason, 
       createdat as CreatedAt, 
       data as Data
from state 
where jobid = @id 
order by id desc;";

            var sqlParameters = new { id = JobId.ToLong(jobId) };
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var job = connectionHolder.Fetch<SqlJob>(jobSql, sqlParameters);

                var parameters = connectionHolder
                    .FetchList<SqlJobParameter>(parametersSql, sqlParameters)
                    .ToDictionary(x => x.Name, x => x.Value);

                var history = connectionHolder
                    .FetchList<SqlState>(stateSql, sqlParameters)
                    .SelectToList(x => new StateHistoryDto
                    {
                        StateName = x.Name,
                        CreatedAt = x.CreatedAt,
                        Reason = x.Reason,
                        Data = SerializationHelper.Deserialize<Dictionary<string, string>>(x.Data)
                    });

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    Job = Utils.DeserializeJob(job.InvocationData, job.Arguments),
                    History = history,
                    Properties = parameters
                };
            }
        }

        public long SucceededListCount()
            => GetNumberOfJobsByStateName(SucceededState.StateName);

        public long DeletedListCount()
            => GetNumberOfJobsByStateName(DeletedState.StateName);

        public StatisticsDto GetStatistics()
        {
            var statistics = new StatisticsDto();
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string stateSql = @"
select statename, count(statename)
from job
where statename in ('Enqueued', 'Failed', 'Processing', 'Scheduled')
group by statename;";
                var stateCounters = connectionHolder.FetchList<(string name, long count)>(stateSql);
                statistics.Enqueued = stateCounters.FirstOrDefault(x => x.name == "Enqueued").count;
                statistics.Failed = stateCounters.FirstOrDefault(x => x.name == "Failed").count;
                statistics.Processing = stateCounters.FirstOrDefault(x => x.name == "Processing").count;
                statistics.Scheduled = stateCounters.FirstOrDefault(x => x.name == "Scheduled").count;

                const string countersSql = @"
select key, sum(value)
from counter
where key in ('stats:succeeded', 'stats:deleted')
group by key;";
                var counters = connectionHolder.FetchList<(string key, long count)>(countersSql);
                statistics.Succeeded = counters.FirstOrDefault(x => x.key == "stats:succeeded").count;
                statistics.Deleted = counters.FirstOrDefault(x => x.key == "stats:deleted").count;

                const string recurringSql = @"
select count(key)
from set
where key = 'recurring-jobs';";
                statistics.Recurring = connectionHolder.FetchScalar<long>(recurringSql);

                statistics.Queues = GetQueues().LongCount();

                statistics.Servers = connectionHolder.FetchScalar<long>("select count(id) from server;");
            }
            return statistics;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = Enumerable.Range(0, 24).Select(i => endDate.AddHours(-i)).ToList();
            var keyMaps = dates.ToDictionary(x => $"stats:{type.ToLowerInvariant()}:{x:yyyy-MM-dd-HH}", x => x);
            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = Enumerable.Range(0, 7).Select(i => endDate.AddDays(-i)).ToList();
            var keyMaps = dates.ToDictionary(x => $"stats:{type.ToLowerInvariant()}:{x:yyyy-MM-dd}", x => x);
            return GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(IDictionary<string, DateTime> keyMaps)
        {
            const string query = @"
select key, count(*) ""count"" 
from counter 
where key = ANY (@keys)
GROUP by key;
";
            Dictionary<string, long> valuesMap;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                valuesMap = connectionHolder.Connection.Query(
                        query,
                        new { keys = keyMaps.Keys.ToList() })
                    .ToList()
                    .ToDictionary(x => (string)x.key, x => (long)x.count);
            }

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key)) valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }

        private long GetNumberOfJobsByStateName(string stateName)
        {
            const string sqlQuery = @"
select count(*) 
from job 
where statename = @state;
";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var parameters = new { state = stateName };
                var count = connectionHolder.Connection.Query<long>(sqlQuery, parameters).Single();
                return count;
            }
        }

        private JobList<TDto> GetJobs<TDto>(int from, int count, string stateName, Utils.JobSelector<TDto> selector, string sorting = AscOrder)
        {
            var query = $@"
select j.id ""Id"",
       j.invocationdata ""InvocationData"",
       j.arguments ""Arguments"", 
       j.createdat ""CreatedAt"", 
       j.expireat ""ExpireAt"",
       null ""FetchedAt"",
       j.statename ""StateName"",
       s.reason ""StateReason"",
       s.data ""StateData""
from job j
LEFT join state s on j.stateid = s.id
where j.statename = @stateName 
order by j.id {sorting} 
limit @count OFFSET @start;
";
            var parameters = new { stateName = stateName, start = @from, count = count };
            var jobs = _connectionProvider.FetchList<SqlJob>(query, parameters);
            return Utils.DeserializeJobs(jobs, selector);
        }

        private const string EnqueuedFetchCondition = "is null";
        private const string FetchedFetchCondition = "is not null";


        private readonly object _cachedQueuesSyncRoot = new object();
        private DateTime _cachedQueuesUpdatedAt = DateTime.MinValue;
        private List<string> _cachedQueues = new List<string>(0);

        public IEnumerable<string> GetQueues()
        {
            if (DateTime.UtcNow - TimeSpan.FromSeconds(5) > _cachedQueuesUpdatedAt)
            {
                lock (_cachedQueuesSyncRoot)
                {
                    if (DateTime.UtcNow - TimeSpan.FromSeconds(5) > _cachedQueuesUpdatedAt)
                    {
                        _cachedQueuesUpdatedAt = DateTime.UtcNow;
                        const string query = @"select DISTINCT queue from jobqueue;";
                        _cachedQueues = _connectionProvider.FetchList<string>(query);
                    }
                }
            }

            return _cachedQueues;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var enqueuedJobsQuery = GetQuery(queue, @from, perPage, EnqueuedState.StateName, EnqueuedFetchCondition);

            var jobs = _connectionProvider.FetchList<SqlJob>(enqueuedJobsQuery);

            return Utils.DeserializeJobs(
                jobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var fetchedJobsQuery = GetQuery(queue, @from, perPage, ProcessingState.StateName, FetchedFetchCondition);

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var jobs = connectionHolder.Connection.Query<SqlJob>(fetchedJobsQuery).ToList();

                return Utils.DeserializeJobs(
                    jobs,
                    (sqlJob, job, stateData) => new FetchedJobDto
                    {
                        Job = Utils.DeserializeJob(sqlJob.InvocationData, sqlJob.Arguments),
                        State = sqlJob.StateName,
                        FetchedAt = sqlJob.FetchedAt
                    });
            }
        }

        private static string GetQuery(string queue, int @from, int perPage, string stateName, string fetchCondition) => $@"
select j.id ""Id"",
       j.invocationdata ""InvocationData"", 
       j.arguments ""Arguments"", 
       j.createdat ""CreatedAt"", 
       j.expireat ""ExpireAt"", 
       s.name ""StateName"", 
       s.reason""StateReason"", 
       s.data ""StateData""
from jobqueue jq
LEFT join job j on jq.jobid = j.id
LEFT join state s on s.id = j.stateid
where jq.queue = '{queue}'
and jq.fetchedat {fetchCondition}
and s.name = '{stateName}'
limit {perPage} OFFSET {from};";

        private EnqueuedAndFetchedJobsCount GetEnqueuedAndFetchedCount(string queue)
        {
            const string query = @"
select count(CASE WHEN fetchedat is null THEN 1 ELSE 0 END) as Enqueued,
       count(CASE WHEN fetchedat is not null THEN 1 ELSE 0 END) as Fetched
from jobqueue
where queue = @queue
";
            return _connectionProvider.Fetch<EnqueuedAndFetchedJobsCount>(query, new { queue = queue });
        }
    }
}
