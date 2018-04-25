using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal class PostgreSqlWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly IPersistentJobQueue _queue;
        private readonly IPostgreSqlConnectionProvider _connectionProvider;
        private readonly Queue<Action<NpgsqlConnection, NpgsqlTransaction>> _commandQueue;

        public PostgreSqlWriteOnlyTransaction(
            IPostgreSqlConnectionProvider connectionProvider,
            IPersistentJobQueue queue)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfNull(queue, nameof(queue));

            _connectionProvider = connectionProvider;
            _queue = queue;
            _commandQueue = new Queue<Action<NpgsqlConnection, NpgsqlTransaction>>();
        }

        public override void Commit()
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.RepeatableRead))
            {
                foreach (var command in _commandQueue)
                {
                    command(connectionHolder.Connection, transaction);
                }
                transaction.Commit();
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            const string sql = @"
UPDATE job
SET expireat = NOW() AT TIME ZONE 'UTC' + @expireIn
WHERE id = @id;
";
            var id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture);
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = id, expireIn = expireIn }, trx));
        }

        public override void PersistJob(string jobId)
        {
            const string sql = @"
UPDATE job
SET expireat = NULL 
WHERE id = @id;
";
            var id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture);
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = id }, trx));
        }

        public override void SetJobState(string jobId, IState state)
        {
            var addAndSetStateSql = $@"
WITH s AS (
    INSERT INTO ""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
    VALUES (@jobId, @name, @reason, @createdAt, @data) RETURNING ""id""
)
UPDATE ""job"" j
SET ""stateid"" = s.""id"", ""statename"" = @name
FROM s
WHERE j.""id"" = @id;
";

            QueueCommand((con, trx) => con.Execute(
                addAndSetStateSql,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData()),
                    id = Convert.ToInt32(jobId, CultureInfo.InvariantCulture)
                }, trx));
        }

        public override void AddJobState(string jobId, IState state)
        {
            var addStateSql = $@"
INSERT INTO ""state"" (""jobid"", ""name"", ""reason"", ""createdat"", ""data"")
VALUES (@jobId, @name, @reason, @createdAt, @data);
";

            QueueCommand((con, trx) => con.Execute(
                addStateSql,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = JobHelper.ToJson(state.SerializeData())
                }, trx));
        }

        public override void AddToQueue(string queue, string jobId) => _queue.Enqueue(queue, jobId);

        public override void IncrementCounter(string key)
        {
            const string sql = @"INSERT INTO ""counter"" (""key"", ""value"") VALUES (@key, @value);";
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = +1 }, trx));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            const string sql = @"
INSERT INTO ""counter""(""key"", ""value"", ""expireat"") 
VALUES (@key, @value, NOW() AT TIME ZONE 'UTC' + @expireIn)";

            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = +1, expireIn = expireIn }, trx));
        }

        public override void DecrementCounter(string key)
        {
            var sql = $@"INSERT INTO ""counter"" (""key"", ""value"") VALUES (@key, @value);";
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { key, value = -1 }, trx));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            const string query = @"
INSERT INTO ""counter""(""key"", ""value"", ""expireat"") 
VALUES (@key, @value, NOW() AT TIME ZONE 'UTC' + @expireIn);";

            QueueCommand((con, trx) => con.Execute(query,
                new { key, value = -1, expireIn = expireIn },
                trx));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0d);
        }

        public override void AddToSet(string key, string value, double score)
        {
            const string query = @"
INSERT INTO ""set"" (""key"", ""value"", ""score"")
VALUES (@key, @value, @score)
ON CONFLICT (""key"", ""value"")
DO UPDATE SET ""score"" = @score
";

            QueueCommand((con, trx) => con.Execute(
                query,
                new { key, value, score }, trx));
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(@"
DELETE FROM ""set"" 
WHERE ""key"" = @key 
AND ""value"" = @value;
",
                new { key, value }, trx));
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(@"
INSERT INTO ""list"" (""key"", ""value"") 
VALUES (@key, @value);
",
                new { key, value }, trx));
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(@"
DELETE FROM ""list"" 
WHERE ""key"" = @key 
AND ""value"" = @value;
",
                new { key, value }, trx));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var trimSql = $@"
DELETE FROM ""list"" AS source
WHERE ""key"" = @key
AND ""id"" NOT IN (
    SELECT ""id"" 
    FROM ""list"" AS keep
    WHERE keep.""key"" = source.""key""
    ORDER BY ""id"" 
    OFFSET @start LIMIT @end
);
";

            QueueCommand((con, trx) => con.Execute(
                trimSql,
                new { key = key, start = keepStartingFrom, end = (keepEndingAt - keepStartingFrom + 1) }, trx));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(keyValuePairs, nameof(keyValuePairs));

            const string query = @"
INSERT INTO ""hash"" (""key"", ""field"", ""value"")
VALUES (@key, @field, @value)
ON CONFLICT (""key"", ""field"")
DO UPDATE SET ""value"" = @value
";

            foreach (var keyValuePair in keyValuePairs)
            {
                var pair = keyValuePair;

                QueueCommand((con, trx) => con.Execute(query, new { key = key, field = pair.Key, value = pair.Value },
                    trx));
            }
        }

        public override void RemoveHash(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"DELETE FROM ""hash"" WHERE ""key"" = @key";
            QueueCommand((con, trx) => con.Execute(
                query,
                new { key }, trx));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"UPDATE ""set"" SET ""expireat"" = @expireAt WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfValueIsNotPositive(expireIn, nameof(expireIn));

            const string query = @"UPDATE ""list"" SET ""expireat"" = @expireAt WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfValueIsNotPositive(expireIn, nameof(expireIn));

            const string query = @"UPDATE ""hash"" SET expireat = @expireAt WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void PersistSet(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"UPDATE ""set"" SET expireat = null WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        public override void PersistList(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"UPDATE ""list"" SET expireat = null WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        public override void PersistHash(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"UPDATE ""hash"" SET expireat = null WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(items, nameof(items));

            const string query = @"INSERT INTO ""set"" (""key"", ""value"", ""score"") VALUES (@key, @value, 0.0)";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                items.Select(value => new { key, value }).ToList(),
                transaction));
        }

        public override void RemoveSet(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            var sql = $@"DELETE FROM ""set"" WHERE ""key"" = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                sql,
                new { key },
                transaction));
        }

        private void QueueCommand(Action<NpgsqlConnection, NpgsqlTransaction> action) => _commandQueue.Enqueue(action);
    }
}
