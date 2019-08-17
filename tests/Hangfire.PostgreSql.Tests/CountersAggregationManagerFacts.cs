using System;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class CountersAggregationManagerFacts
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly CountersAggregationManager _manager;

        public CountersAggregationManagerFacts()
        {
            _connectionProvider = ConnectionUtils.GetConnectionProvider();
            _manager = new CountersAggregationManager(_connectionProvider, TimeSpan.FromMilliseconds(1));
        }

        [Fact, CleanDatabase]
        public void Execute_Aggregates_CounterTable()
        {
            // Arrange
            const string createSql = @"insert into counter (key, value) values ('stats:succeeded', 1)";
            const int sum = 5;
            for (int i = 0; i < sum; i++)
            {
                _connectionProvider.Execute(createSql);
            }

            // Act
            _manager.Execute(CancellationToken.None);

            // Assert
            const string checkQuery = @"select count(*), sum(value) from counter";
            var (recordsCount, recordsValue) = _connectionProvider.Fetch<(long, long)>(checkQuery);
            Assert.Equal(1, recordsCount);
            Assert.Equal(sum, recordsValue);
        }
    }
}
