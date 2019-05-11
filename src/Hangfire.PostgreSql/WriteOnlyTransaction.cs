using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.States;
using Hangfire.Storage;
using Npgsql;

namespace Hangfire.PostgreSql
{
    internal sealed class WriteOnlyTransaction : JobStorageTransaction
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly Queue<Action<NpgsqlConnection, NpgsqlTransaction>> _commandQueue;

        public WriteOnlyTransaction(IConnectionProvider connectionProvider)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));

            _connectionProvider = connectionProvider;
            _commandQueue = new Queue<Action<NpgsqlConnection, NpgsqlTransaction>>();
        }

        public override void Commit()
        {
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            using (var transaction = connectionHolder.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
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
update job
set expireat = now() at time zone 'UTC' + @expireIn
where id = @id;
";
            var id = JobId.ToLong(jobId);
            QueueCommand((con, trx) => con.Execute(
                sql,
                new { id = id, expireIn = expireIn }, trx));
        }

        public override void PersistJob(string jobId)
        {
            const string query = @"
update job
set expireat = null 
where id = @id;
";
            var id = JobId.ToLong(jobId);
            QueueCommand((con, trx) => con.Execute(
                query,
                new { id = id }, trx));
        }

        public override void SetJobState(string jobId, IState state)
        {
            const string query = @"
with s as (
    insert into state (jobid, name, reason, createdat, data)
    values (@jobId, @name, @reason, @createdAt, @data) returning id
)
update job j
set stateid = s.id, statename = @name
from s
where j.id = @id;
";

            QueueCommand((con, trx) => con.Execute(
                query,
                new
                {
                    id = JobId.ToLong(jobId),
                    jobId = JobId.ToLong(jobId),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = SerializationHelper.Serialize(state.SerializeData())
                }, trx));
        }

        public override void AddJobState(string jobId, IState state)
        {
            const string query = @"
insert into state (jobid, name, reason, createdat, data)
values (@jobId, @name, @reason, @createdAt, @data);
";

            QueueCommand((con, trx) => con.Execute(
                query,
                new
                {
                    jobId = Convert.ToInt32(jobId, CultureInfo.InvariantCulture),
                    name = state.Name,
                    reason = state.Reason,
                    createdAt = DateTime.UtcNow,
                    data = SerializationHelper.Serialize(state.SerializeData())
                }, trx));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            const string query = @"insert into jobqueue (jobid, queue) values (@jobId, @queue);";
            var parameters = new { jobId = JobId.ToLong(jobId), queue = queue };

            QueueCommand((con, trx) => con.Execute(query, parameters, trx));
        }

        public override void IncrementCounter(string key)
        {
            const string query = @"insert into counter (key, value) values (@key, @value);";
            QueueCommand((con, trx) => con.Execute(
                query,
                new { key, value = +1 }, trx));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            const string query = @"
insert into counter(key, value, expireat) 
values (@key, @value, now() at time zone 'UTC' + @expireIn)";

            QueueCommand((con, trx) => con.Execute(
                query,
                new { key, value = +1, expireIn = expireIn }, trx));
        }

        public override void DecrementCounter(string key)
        {
            const string query = @"insert into counter (key, value) values (@key, @value);";
            QueueCommand((con, trx) => con.Execute(
                query,
                new { key, value = -1 }, trx));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            const string query = @"
insert into counter(key, value, expireat) 
values (@key, @value, now() at time zone 'UTC' + @expireIn);";

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
insert into set (key, value, score)
values (@key, @value, @score)
on conflict (key, value)
do update set score = @score
";

            QueueCommand((con, trx) => con.Execute(
                query,
                new { key, value, score }, trx));
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(@"
delete from set 
where key = @key 
and value = @value;
",
                new { key, value }, trx));
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(@"
insert into list (key, value) 
values (@key, @value);
",
                new { key, value }, trx));
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand((con, trx) => con.Execute(@"
delete from list 
where key = @key 
and value = @value;
",
                new { key, value }, trx));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            const string trimSql = @"
delete from list as source
where key = @key
and id not IN (
    select id 
    from list as keep
    where keep.key = source.key
    order by id 
    OFFSET @start limit @end
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
insert into hash (key, field, value)
values (@key, @field, @value)
on conflict (key, field)
do update set value = @value
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

            const string query = @"delete from hash where key = @key";
            QueueCommand((con, trx) => con.Execute(
                query,
                new { key }, trx));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"update set set expireat = @expireAt where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfValueIsNotPositive(expireIn, nameof(expireIn));

            const string query = @"update list set expireat = @expireAt where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfValueIsNotPositive(expireIn, nameof(expireIn));

            const string query = @"update hash set expireat = @expireAt where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key, expireAt = DateTime.UtcNow.Add(expireIn) },
                transaction));
        }

        public override void PersistSet(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"update set set expireat = null where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        public override void PersistList(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"update list set expireat = null where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        public override void PersistHash(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"update hash set expireat = null where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(items, nameof(items));

            const string query = @"
insert into set (key, value, score)
values (@key, @value, 0.0)
on conflict (key, value)
do nothing";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                items.Select(value => new { key, value }).ToList(),
                transaction));
        }

        public override void RemoveSet(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"delete from set where key = @key";

            QueueCommand((connection, transaction) => connection.Execute(
                query,
                new { key },
                transaction));
        }

        private void QueueCommand(Action<NpgsqlConnection, NpgsqlTransaction> action) => _commandQueue.Enqueue(action);
    }
}
