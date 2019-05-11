using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Storage
{
    internal sealed partial class StorageConnection : JobStorageConnection
    {
        public override List<string> GetAllItemsFromList(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT value FROM list WHERE key = @key ORDER BY id DESC";
            return _connectionProvider.FetchList<string>(query, new { key = key });
        }

        public override long GetListCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT COUNT(id) FROM list WHERE key = @key";
            return _connectionProvider.FetchFirstOrDefault<long>(query, new { key = key });
        }

        public override TimeSpan GetListTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT MIN(expireat) FROM list WHERE key = @key";

            var result = _connectionProvider.FetchFirstOrDefault<DateTime?>(query, new { key = key });

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"
SELECT value FROM (
    SELECT value, row_number() OVER (ORDER BY id DESC) AS row_num 
    FROM list
    WHERE key = @key ) AS s
WHERE s.row_num BETWEEN @startingFrom AND @endingAt";

            var parameters = new
            {
                key = key,
                startingFrom = startingFrom + 1,
                endingAt = endingAt + 1
            };
            return _connectionProvider.FetchList<string>(query, parameters);
        }
    }
}
