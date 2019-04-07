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
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    internal sealed class StorageConnection : JobStorageConnection
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
INSERT INTO jobparameter (""jobid"", ""name"", ""value"")
VALUES (@jobId, @name , @value)
ON CONFLICT (""jobid"", ""name"")
DO UPDATE SET ""value"" = @value
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

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            const string query = @"SELECT ""value"" FROM ""set"" WHERE ""key"" = @key;";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<string>(query, new { key = key });
                return new HashSet<string>(result);
            }
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");


            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                const string query = @"
SELECT ""value"" 
FROM ""set"" 
WHERE ""key"" = @key 
AND ""score"" BETWEEN @from AND @to 
ORDER BY ""score"" LIMIT 1;
";
                return connectionHolder.Connection.Query<string>(query, new { key, from = fromScore, to = toScore })
                    .SingleOrDefault();
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            const string query = @"
INSERT INTO hash(key, field, value)
VALUES (@key, @field, @value)
ON CONFLICT (key, field)
DO UPDATE SET value = @value
";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                foreach (var keyValuePair in keyValuePairs)
                {
                    connectionHolder.Connection.Execute(
                        query,
                        new { key = key, field = keyValuePair.Key, value = keyValuePair.Value });
                }
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                const string query = @"
SELECT field AS Field, value AS Value 
FROM hash 
WHERE key = @key
;";
                var result = transaction.Connection.Query<SqlHash>(
                        query,
                        new { key = key },
                        transaction)
                    .ToDictionary(x => x.Field, x => x.Value);
                transaction.Commit();

                return result.Count != 0 ? result : null;
            }
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));
            Guard.ThrowIfNull(context, nameof(context));

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            const string query = @"
INSERT INTO server (id, data, lastheartbeat)
VALUES (@id, @data, NOW() AT TIME ZONE 'UTC')
ON CONFLICT (id)
DO UPDATE SET data = @data, lastheartbeat = NOW() AT TIME ZONE 'UTC'
";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(query,
                    new { id = serverId, data = SerializationHelper.Serialize(data) });
            }
        }

        public override void RemoveServer(string serverId)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(@"DELETE FROM server WHERE id = @id;", new { id = serverId });
            }
        }

        public override void Heartbeat(string serverId)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));

            const string query = @"
UPDATE server 
SET lastheartbeat = NOW() AT TIME ZONE 'UTC' 
WHERE id = @id;";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                connectionHolder.Connection.Execute(query, new { id = serverId });
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            Guard.ThrowIfValueIsNotPositive(timeOut, nameof(timeOut));

            const string query = @"DELETE FROM server WHERE lastheartbeat < (NOW() AT TIME ZONE 'UTC' - @timeout);";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Execute(query, new { timeout = timeOut });
            }
        }

        public override long GetSetCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT COUNT(key) FROM set WHERE key = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long>(query, new { key }).First();
            }
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT value FROM list WHERE key = @key ORDER BY id DESC";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query, new { key }).ToList();
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

        public override long GetListCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select count(""id"") from ""list"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long>(query, new { key }).SingleOrDefault();
            }
        }

        public override TimeSpan GetListTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select min(""expireat"") from ""list"" where ""key"" = @key";
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<DateTime?>(query, new { key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"
select ""value"" from (
    select ""value"", row_number() over (order by ""id"" desc) as row_num 
    from ""list""
    where ""key"" = @key ) as s
where s.row_num between @startingFrom and @endingAt";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection
                    .Query<string>(query, new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 })
                    .ToList();
            }
        }

        public override long GetHashCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select count(""id"") from ""hash"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<long>(query, new { key }).SingleOrDefault();
            }
        }

        public override TimeSpan GetHashTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select min(""expireat"") from ""hash"" where ""key"" = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<DateTime?>(query, new { key }).Single();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"
select ""value"" from (
    select ""value"", row_number() over (order by ""id"" ASC) as row_num 
    from ""set""
    where ""key"" = @key 
    ) as s
where s.row_num between @startingFrom and @endingAt";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query,
                        new { key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 })
                    .ToList();
            }
        }

        public override TimeSpan GetSetTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT MIN(expireat) FROM set WHERE key = @key";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<DateTime?>(query, new { key }).SingleOrDefault();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }
        }

        public override string GetValueFromHash(string key, string name)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(name, nameof(name));

            const string query = @"select ""value"" from ""hash"" where ""key"" = @key and ""field"" = @field";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query, new { key, field = name }).SingleOrDefault();
            }
        }
    }
}
