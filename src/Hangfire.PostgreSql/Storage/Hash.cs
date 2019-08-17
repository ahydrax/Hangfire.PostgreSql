using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Storage
{
    internal sealed partial class StorageConnection : JobStorageConnection
    {
        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"
select field as Field, value as Value 
from hash 
where key = @key
;";

            var result = _connectionProvider
                .FetchList<SqlHash>(query, new { key = key })
                .ToDictionary(x => x.Field, x => x.Value);

            return result.Count != 0 ? result : null;
        }

        public override long GetHashCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select count(*) from hash where key = @key";

            return _connectionProvider.FetchFirstOrDefault<long>(query, new { key });
        }

        public override TimeSpan GetHashTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select min(expireat) from hash where key = @key";

            var result = _connectionProvider.FetchFirstOrDefault<DateTime?>(query, new { key });
            if (!result.HasValue) return TimeSpan.FromSeconds(-1);

            return result.Value - DateTime.UtcNow;
        }

        public override string GetValueFromHash(string key, string name)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(name, nameof(name));

            const string query = @"select value from hash where key = @key and field = @field";

            return _connectionProvider.FetchFirstOrDefault<string>(query, new { key, field = name });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            Guard.ThrowIfNull(key, nameof(key));
            Guard.ThrowIfNull(keyValuePairs, nameof(keyValuePairs));

            const string query = @"
insert into hash(key, field, value)
values (@key, @field, @value)
on conflict (key, field)
do update set value = @value
";
            var parameters = keyValuePairs.Select(x => new { key = key, field = x.Key, value = x.Value }).ToArray();
            _connectionProvider.Execute(query, parameters);
        }
    }
}
