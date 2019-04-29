using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Storage
{
    internal sealed partial class StorageConnection : JobStorageConnection
    {


        public override List<string> GetAllItemsFromList(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"SELECT value FROM list WHERE key = @key ORDER BY id DESC";

            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Query<string>(query, new { key }).ToList();
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
    }
}