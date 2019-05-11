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

            const string query = @"select value from list where key = @key order by id desc";
            return _connectionProvider.FetchList<string>(query, new { key = key });
        }

        public override long GetListCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select count(id) from list where key = @key";
            return _connectionProvider.FetchFirstOrDefault<long>(query, new { key = key });
        }

        public override TimeSpan GetListTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select min(expireat) from list where key = @key";

            var result = _connectionProvider.FetchFirstOrDefault<DateTime?>(query, new { key = key });

            return result.HasValue ? result.Value - DateTime.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"
select value from (
    select value, row_number() over (order by id desc) as row_num 
    from list
    where key = @key ) as s
where s.row_num between @startingFrom and @endingAt";

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
