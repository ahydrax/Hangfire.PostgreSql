using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Storage
{
    internal sealed partial class StorageConnection : JobStorageConnection
    {
        
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

    }
}
