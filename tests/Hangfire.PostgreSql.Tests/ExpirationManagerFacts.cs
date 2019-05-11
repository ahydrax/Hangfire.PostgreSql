using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Tests.Utils;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class ExpirationManagerFacts
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly ExpirationManager _expirationManager;
        private readonly CancellationTokenSource _tokenSource;

        public ExpirationManagerFacts()
        {
            _connectionProvider = ConnectionUtils.GetConnectionProvider();
            _expirationManager = new ExpirationManager(_connectionProvider, TimeSpan.FromMilliseconds(1));
            _tokenSource = new CancellationTokenSource();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionProviderIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => new ExpirationManager(null));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsZero()
        {
            Assert.Throws<ArgumentException>(
                () => new ExpirationManager(_connectionProvider, TimeSpan.Zero));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsNegative()
        {
            Assert.Throws<ArgumentException>(
                () => new ExpirationManager(_connectionProvider, TimeSpan.FromSeconds(-1)));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            // Arrange
            CreateCounterExpirationEntry(DateTime.UtcNow.AddDays(-1));

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from counter");
            Assert.Equal(0, recordsCount);
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            // Arrange
            CreateCounterExpirationEntry(null);

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from counter");
            Assert.Equal(1, recordsCount);
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            // Arrange
            CreateCounterExpirationEntry(DateTime.UtcNow.AddDays(1));

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from counter");
            Assert.Equal(1, recordsCount);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_CounterTable()
        {
            // Arrange
            CreateCounterExpirationEntry(DateTime.UtcNow.AddDays(-1));

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from counter");
            Assert.Equal(0, recordsCount);
        }

        private void CreateCounterExpirationEntry(DateTime? expireAt)
        {
            const string query = @"
insert into counter(key, value, expireat)
values ('key', 1, @expireat)";

            _connectionProvider.Execute(query, new { expireat = expireAt });
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            // Arrange
            const string query = @"
insert into job (invocationdata, arguments, createdat, expireat) 
values ('', '', now() at time zone 'utc', @expireAt)";
            _connectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from job");
            Assert.Equal(0, recordsCount);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            // Arrange
            const string query = @"
insert into list (key, expireat) 
values ('key', @expireAt)";
            _connectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from list");
            Assert.Equal(0, recordsCount);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            // Arrange
            const string query = @"
insert into set (key, score, value, expireat) 
values ('key', 0, '', @expireAt)";
            _connectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from set");
            Assert.Equal(0, recordsCount);
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            // Arrange
            const string query = @"
insert into hash (key, field, value, expireat) 
values ('key', 'field', '', @expireAt)";
            _connectionProvider.Execute(query, new { expireAt = DateTime.UtcNow.AddDays(-1) });

            // Act
            _expirationManager.Execute(_tokenSource.Token);

            // Assert
            var recordsCount = _connectionProvider.Fetch<int>("select count(*) from set");
            Assert.Equal(0, recordsCount);
        }
    }
}
