using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Storage
{
    internal sealed partial class StorageConnection : JobStorageConnection
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly IJobQueue _queue;

        public StorageConnection(
            IConnectionProvider connectionProvider,
            IJobQueue queue,
            PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(queue, nameof(queue));
            Guard.ThrowIfNull(options, nameof(options));

            _connectionProvider = connectionProvider;
            _queue = queue;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
            => new WriteOnlyTransaction(_connectionProvider);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
            => new DistributedLock(resource, timeout, _connectionProvider);

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            Guard.ThrowIfCollectionIsNullOrEmpty(queues, nameof(queues));
            return _queue.Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            Guard.ThrowIfNull(job, nameof(job));
            Guard.ThrowIfNull(parameters, nameof(parameters));
            Guard.ThrowIfValueIsNotPositive(expireIn, nameof(expireIn));

            const string createJobSql = @"
insert into job (invocationdata, arguments, createdat, expireat)
values (@invocationData, @arguments, @createdAt, @expireAt) 
returning id;
";
            var invocationData = InvocationData.SerializeJob(job);

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                var createJobParameters = new
                {
                    invocationData = SerializationHelper.Serialize(invocationData),
                    arguments = invocationData.Arguments,
                    createdAt = createdAt,
                    expireAt = createdAt.Add(expireIn)
                };
                var jobId = connectionHolder.FetchFirstOrDefault<long>(createJobSql, createJobParameters, transaction);

                if (parameters.Count > 0)
                {
                    var parametersArray = parameters.Select(x => new
                    {
                        jobId = jobId,
                        name = x.Key,
                        value = x.Value
                    }).ToArray();

                    const string insertParameterSql = @"
insert into jobparameter (jobid, name, value)
values (@jobId, @name, @value);
";
                    connectionHolder.Execute(insertParameterSql, parametersArray, transaction);
                }
                transaction.Commit();
                return JobId.ToString(jobId);
            }
        }

        public override JobData GetJobData(string jobId)
        {
            Guard.ThrowIfNull(jobId, nameof(jobId));

            const string sql = @"
select invocationdata as invocationData, statename as stateName, arguments, createdat as createdAt
from job 
where id = @id;
";
            var jobData = _connectionProvider.FetchFirstOrDefault<SqlJob>(sql, new { id = JobId.ToLong(jobId) });

            if (jobData == null) return null;

            // TODO: conversion exception could be thrown.
            var invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData);
            if (!string.IsNullOrEmpty(jobData.Arguments))
            {
                invocationData.Arguments = jobData.Arguments;
            }

            Job job = null;
            JobLoadException loadException = null;
            try
            {
                job = invocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobData.StateName,
                CreatedAt = jobData.CreatedAt,
                LoadException = loadException
            };
        }

        public override StateData GetStateData(string jobId)
        {
            Guard.ThrowIfNull(jobId, nameof(jobId));

            const string query = @"
select s.name as Name, s.reason as Reason, s.data as Data
from state s
inner join job j on j.stateid = s.id
where j.id = @jobId;
";

            var sqlState = _connectionProvider.FetchFirstOrDefault<SqlState>(query, new { jobId = JobId.ToLong(jobId) });
            if (sqlState == null) return null;

            return new StateData
            {
                Name = sqlState.Name,
                Reason = sqlState.Reason,
                Data = SerializationHelper.Deserialize<Dictionary<string, string>>(sqlState.Data)
            };
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Guard.ThrowIfNull(id, nameof(id));
            Guard.ThrowIfNull(name, nameof(name));

            const string query = @"
insert into jobparameter (jobid, name, value)
values (@jobId, @name , @value)
on conflict (jobid, name)
do update set value = @value
";

            var parameters = new { jobId = JobId.ToLong(id), name, value };
            _connectionProvider.Execute(query, parameters);
        }

        public override string GetJobParameter(string id, string name)
        {
            Guard.ThrowIfNull(id, nameof(id));
            Guard.ThrowIfNull(name, nameof(name));

            var jobId = JobId.ToLong(id);
            const string query = @"select value from jobparameter where jobid = @id and name = @name limit 1;";
            return _connectionProvider.FetchFirstOrDefault<string>(query, new { id = jobId, name = name });
        }

        public override long GetCounter(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select sum(value) from counter where key = @key";
            return _connectionProvider.FetchFirstOrDefault<long?>(query, new { key }) ?? 0;
        }
    }
}
