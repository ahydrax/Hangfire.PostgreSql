using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Storage;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class DistributedLockFacts
    {
        private readonly IConnectionProvider _connectionProvider;

        public DistributedLockFacts()
        {
            _connectionProvider = ConnectionUtils.GetConnectionProvider();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNullOrEmpty()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DistributedLock("", TimeSpan.FromSeconds(1), _connectionProvider));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTimeoutIsNegativeValue()
        {
            var exception = Assert.Throws<ArgumentException>(
                () => new DistributedLock("hello", TimeSpan.FromSeconds(-1), _connectionProvider));

            Assert.Equal("timeout", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionProviderIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new DistributedLock("hello", TimeSpan.FromSeconds(1), null));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquiresExclusiveApplicationLock()
        {
            // Arrange
            using (new DistributedLock("hello", TimeSpan.FromSeconds(1), _connectionProvider))
            {
                // Act
                var lockCount = _connectionProvider.FetchScalar<long>(
                    @"select count(*) from lock where resource = @resource;",
                    new { resource = "hello" });

                // Assert
                Assert.Equal(1, lockCount);
            }
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfLockCanNotBeGranted()
        {
            // Arrange
            var timeout = TimeSpan.FromSeconds(3);
            var releaseLock = new ManualResetEventSlim(false);
            var lockAcquired = new ManualResetEventSlim(false);
            var thread = new Thread(
                () =>
                {
                    using (new DistributedLock("exclusive", timeout, _connectionProvider))
                    {
                        lockAcquired.Set();
                        releaseLock.Wait();
                    }
                });

            // Act
            thread.Start();
            lockAcquired.Wait();

            // Assert
            Assert.Throws<DistributedLockTimeoutException>(
                () => new DistributedLock("exclusive", timeout, _connectionProvider));

            releaseLock.Set();
            thread.Join();
        }

        [Fact, CleanDatabase]
        public void Dispose_ReleasesExclusiveApplicationLock()
        {
            // Arrange
            var distributedLock = new DistributedLock("hello", TimeSpan.FromSeconds(1), _connectionProvider);

            // Act
            distributedLock.Dispose();

            // Assert
            var lockCount = _connectionProvider.FetchScalar<long>(
                @"select count(*) from lock where resource = @resource;",
                new { resource = "hello" });

            Assert.Equal(0, lockCount);
        }

        [Fact, CleanDatabase]
        public void Dispose_ThrowsExceptionWhenLockWasDeletedExternally()
        {
            // Arrange
            var distributedLock = new DistributedLock("hello", TimeSpan.FromSeconds(1), _connectionProvider);

            // Act
            _connectionProvider.Execute("delete from lock;");

            // Assert
            Assert.Throws<DistributedLockException>(() => distributedLock.Dispose());
        }
    }
}
