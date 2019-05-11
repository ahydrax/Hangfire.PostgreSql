using System;
using System.Linq;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Storage;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class PostgreSqlStorageFacts
    {
        private readonly PostgreSqlStorageOptions _options;

        public PostgreSqlStorageFacts()
        {
            _options = new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = false };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlStorage(connectionString: null));

            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionBuilderIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlStorage(connectionBuilder: null));

            Assert.Equal("connectionBuilder", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new PostgreSqlStorage(ConnectionUtils.GetConnectionString(), null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            var api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact]
        public void GetConnection_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            using (var connection = (StorageConnection)storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            var storage = CreateStorage();

            var components = storage.GetComponents();

            var componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(ExpirationManager), componentTypes);
            Assert.Contains(typeof(ExpiredLocksManager), componentTypes);
            Assert.Contains(typeof(CountersAggregationManager), componentTypes);
        }

        private PostgreSqlStorage CreateStorage()
            => new PostgreSqlStorage(ConnectionUtils.GetConnectionString(), _options);
    }
}
