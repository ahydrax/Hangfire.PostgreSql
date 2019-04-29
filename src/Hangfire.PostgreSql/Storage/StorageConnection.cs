using System;
using System.Collections.Generic;
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
            => new WriteOnlyTransaction(_connectionProvider, _queue);

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
            => new DistributedLock(resource, timeout, _connectionProvider);

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));
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

            const string createJobSql = @"
INSERT INTO job (invocationdata, arguments, createdat, expireat)
VALUES (@invocationData, @arguments, @createdAt, @expireAt) 
RETURNING id;
";
            var invocationData = InvocationData.SerializeJob(job);

            int jobId;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var connection = connectionHolder.Connection;
                jobId = connection.Query<int>(
                    createJobSql,
                    new
                    {
                        invocationData = SerializationHelper.Serialize(invocationData),
                        arguments = invocationData.Arguments,
                        createdAt = createdAt,
                        expireAt = createdAt.Add(expireIn)
                    }).Single();
            }

            if (parameters.Count > 0)
            {
                var parameterArray = new object[parameters.Count];
                var parameterIndex = 0;
                foreach (var parameter in parameters)
                {
                    parameterArray[parameterIndex++] = new
                    {
                        jobId = jobId,
                        name = parameter.Key,
                        value = parameter.Value
                    };
                }

                const string insertParameterSql = @"
INSERT INTO jobparameter (jobid, name, value)
VALUES (@jobId, @name, @value);
";
                using (var connectionHolder = _connectionProvider.AcquireConnection())
                {
                    connectionHolder.Connection.Execute(insertParameterSql, parameterArray);
                }
            }
            return jobId.ToString(CultureInfo.InvariantCulture);
        }

        public override JobData GetJobData(string jobId)
        {
            Guard.ThrowIfNull(jobId, nameof(jobId));

            const string sql = @"
SELECT ""invocationdata"" ""invocationData"", ""statename"" ""stateName"", ""arguments"", ""createdat"" ""createdAt"" 
FROM job 
WHERE ""id"" = @id;
";

            SqlJob jobData;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                jobData = connectionHolder.Connection
                    .Query<SqlJob>(sql, new { id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                    .SingleOrDefault();
            }

            if (jobData == null) return null;

            // TODO: conversion exception could be thrown.
            var invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData);
            invocationData.Arguments = jobData.Arguments;

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
SELECT s.name ""Name"", s.reason ""Reason"", s.data ""Data""
FROM state s
INNER JOIN job j on j.stateid = s.id
WHERE j.id = @jobId;
";

            SqlState sqlState;
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                sqlState = connectionHolder.Connection
                    .Query<SqlState>(query, new { jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture) })
                    .SingleOrDefault();
            }

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
INSERT INTO jobparameter (jobid, name, value)
VALUES (@jobId, @name , @value)
ON CONFLICT (jobid, name)
DO UPDATE SET value = @value
";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var parameters = new { jobId = Convert.ToInt32(id, CultureInfo.InvariantCulture), name, value };
                connectionHolder.Connection.Execute(query, parameters);
            }
        }

        public override string GetJobParameter(string id, string name)
        {
            Guard.ThrowIfNull(id, nameof(id));
            Guard.ThrowIfNull(name, nameof(name));

            const string query = @"SELECT value FROM jobparameter WHERE jobid = @id AND name = @name;";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var parameters = new { id = Convert.ToInt32(id, CultureInfo.InvariantCulture), name = name };
                return connectionHolder.Connection.Query<string>(query, parameters).SingleOrDefault();
            }
        }

        public override long GetCounter(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select sum(""value"") as ""Value"" from ""counter"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long?>(query, new { key }).SingleOrDefault() ?? 0;
            }
        }



    }
}
