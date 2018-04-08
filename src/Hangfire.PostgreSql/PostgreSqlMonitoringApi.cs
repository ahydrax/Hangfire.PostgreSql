﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Entities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

// ReSharper disable RedundantAnonymousTypePropertyName
namespace Hangfire.PostgreSql
{
    internal class PostgreSqlMonitoringApi : IMonitoringApi
    {
        private const string AscOrder = "ASC";
        private const string DescOrder = "DESC";

        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly IPersistentJobQueueMonitoringApi _queueMonitoringApi;
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlMonitoringApi(
            IPostgreSqlConnectionProvider connection,
            IPersistentJobQueueMonitoringApi queueMonitoringApi,
            PostgreSqlStorageOptions options)
        {
            _connectionProvider = connection ?? throw new ArgumentNullException(nameof(connection));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _queueMonitoringApi = queueMonitoringApi ?? throw new ArgumentNullException(nameof(queueMonitoringApi));
        }

        public long ScheduledCount()
            => GetNumberOfJobsByStateName(ScheduledState.StateName);

        public long EnqueuedCount(string queue)
            => _queueMonitoringApi.GetEnqueuedAndFetchedCount(queue).enqueued ?? 0;

        public long FetchedCount(string queue)
            => _queueMonitoringApi.GetEnqueuedAndFetchedCount(queue).fetched ?? 0;

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
                const string query = @"SELECT * FROM server";
                serverDtos = connectionHolder.Connection.Query<Entities.Server>(query).ToList();
            }

            var servers = new List<ServerDto>(serverDtos.Count);
            foreach (var server in serverDtos)
            {
                var data = JobHelper.FromJson<ServerData>(server.Data);
                servers.Add(new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.LastHeartbeat,
                    Queues = data.Queues,
                    StartedAt = data.StartedAt ?? DateTime.MinValue,
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
                           ? (long?)long.Parse(stateData["PerformanceDuration"]) +
                            (long?)long.Parse(stateData["Latency"])
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
            var queues = _queueMonitoringApi.GetQueues().ToArray();

            var queueInfos = new List<QueueWithTopEnqueuedJobsDto>(queues.Length);
            foreach (var queue in queues)
            {
                var counters = _queueMonitoringApi.GetEnqueuedAndFetchedCount(queue);
                var firstJobs = EnqueuedJobs(queue, 0, 5);

                queueInfos.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue,
                    Length = counters.enqueued ?? 0,
                    Fetched = counters.fetched ?? 0,
                    FirstJobs = firstJobs
                });
            }

            return queueInfos;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
            => _queueMonitoringApi.EnqueuedJobs(queue, from, perPage);

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
            => _queueMonitoringApi.FetchedJobs(queue, from, perPage);

        public IDictionary<DateTime, long> HourlySucceededJobs()
            => GetHourlyTimelineStats(SucceededState.StateName);

        public IDictionary<DateTime, long> HourlyFailedJobs()
            => GetHourlyTimelineStats(FailedState.StateName);

        public JobDetailsDto JobDetails(string jobId)
        {
            const string sql = @"
SELECT id ""Id"", 
       invocationdata ""InvocationData"", 
       arguments ""Arguments"", 
       createdat ""CreatedAt"", 
       expireat ""ExpireAt"" 
FROM job
WHERE id = @id;

SELECT jobid ""JobId"", 
       name ""Name"",
       value ""Value"" 
FROM jobparameter 
WHERE jobid = @id;

SELECT jobid ""JobId"", 
       name ""Name"", 
       reason ""Reason"", 
       createdat ""CreatedAt"", 
       data ""Data"" 
FROM state 
WHERE jobid = @id 
ORDER BY id DESC;
";
            var sqlParameters = new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) };

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var multi = connectionHolder.Connection.QueryMultiple(sql, sqlParameters))
            {
                var job = multi.Read<SqlJob>().SingleOrDefault();
                if (job == null) return null;

                var parameters = multi.Read<JobParameter>().ToDictionary(x => x.Name, x => x.Value);
                var history = multi.Read<SqlState>()
                                   .ToList()
                                   .Select(x => new StateHistoryDto
                                   {
                                       StateName = x.Name,
                                       CreatedAt = x.CreatedAt,
                                       Reason = x.Reason,
                                       Data = JobHelper.FromJson<Dictionary<string, string>>(x.Data)
                                   })
                                   .ToList();

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
            const string sql = @"
SELECT statename ""State"", 
       COUNT(*) ""Count"" 
FROM job
WHERE statename IS NOT NULL
GROUP BY statename;

SELECT COUNT(*) 
FROM server;

SELECT SUM(value) 
FROM counter 
WHERE key = 'stats:succeeded';

SELECT SUM(value) 
FROM counter 
WHERE key = 'stats:deleted';

SELECT COUNT(*) 
FROM set 
WHERE key = 'recurring-jobs';
";

            var stats = new StatisticsDto();
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var gridReader = connectionHolder.Connection.QueryMultiple(sql))
            {
                var countByStates = gridReader.Read().ToDictionary(x => x.State, x => x.Count);

                long GetCountIfExists(string name) => countByStates.ContainsKey(name) ? countByStates[name] : 0;

                stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
                stats.Failed = GetCountIfExists(FailedState.StateName);
                stats.Processing = GetCountIfExists(ProcessingState.StateName);
                stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

                stats.Servers = gridReader.Read<long>().Single();

                stats.Succeeded = gridReader.Read<long?>().SingleOrDefault() ?? 0;
                stats.Deleted = gridReader.Read<long?>().SingleOrDefault() ?? 0;

                stats.Recurring = gridReader.Read<long>().Single();
            }

            stats.Queues = _queueMonitoringApi.GetQueues().Count();

            return stats;

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
SELECT key, COUNT(*) ""count"" 
FROM counter 
WHERE key = ANY (@keys)
GROUP BY key;
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
SELECT COUNT(*) 
FROM job 
WHERE statename = @state;
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
SELECT j.id ""Id"",
       j.invocationdata ""InvocationData"",
       j.arguments ""Arguments"", 
       j.createdat ""CreatedAt"", 
       j.expireat ""ExpireAt"",
       NULL ""FetchedAt"",
       j.statename ""StateName"",
       s.reason ""StateReason"",
       s.data ""StateData""
FROM job j
LEFT JOIN state s ON j.stateid = s.id
WHERE j.statename = @stateName 
ORDER BY j.id {sorting} 
LIMIT @count OFFSET @start;
";
            var parameters = new { stateName = stateName, start = @from, count = count };
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var jobs = connectionHolder.Connection.Query<SqlJob>(query, parameters).ToList();
                return Utils.DeserializeJobs(jobs, selector);
            }
        }
    }
}
