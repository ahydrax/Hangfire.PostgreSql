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
        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select value from set where key = @key order by score;";
            var result = _connectionProvider.FetchList<string>(query, new { key = key });
            return new HashSet<string>(result);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            Guard.ThrowIfNull(key, nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            const string query = @"
select value 
from set 
where key = @key 
and score between @from and @to 
order by score limit 1;
";
            return _connectionProvider.FetchFirstOrDefault<string>(query, new { key, from = fromScore, to = toScore });
        }

        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            Guard.ThrowIfNull(key, nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            if (count < 1) throw new ArgumentException("The `count` must be equal or greater than 1.");

            var query = $@"
select value 
from set 
where key = @key 
and score between @from and @to 
order by score limit {count};";
            return _connectionProvider.FetchList<string>(query, new { key, from = fromScore, to = toScore });
        }

        public override long GetSetCount(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select count(key) from set where key = @key";
            return _connectionProvider.FetchFirstOrDefault<long>(query, new { key });
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"
select value from (
    select value, row_number() over (order by score asc) as row_num 
    from set
    where key = @key 
    ) as s
where s.row_num between @startingFrom and @endingAt";

            return _connectionProvider.FetchList<string>(query,
                    new
                    {
                        key = key,
                        startingFrom = startingFrom + 1,
                        endingAt = endingAt + 1
                    });
        }


        public override TimeSpan GetSetTtl(string key)
        {
            Guard.ThrowIfNull(key, nameof(key));

            const string query = @"select min(expireat) from set where key = @key";
            var result = _connectionProvider.FetchFirstOrDefault<DateTime?>(query, new { key });

            if (!result.HasValue) return TimeSpan.FromSeconds(-1);
            return result.Value - DateTime.UtcNow;
        }
    }
}
