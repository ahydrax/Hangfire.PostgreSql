using System;
using System.Linq;
using System.Threading;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.States;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class MonitoringApiFacts
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly MonitoringApi _monitoringApi;

        public MonitoringApiFacts()
        {
            _connectionProvider = ConnectionUtils.GetConnectionProvider();
            _monitoringApi = new MonitoringApi(_connectionProvider);
        }

        [Fact, CleanDatabase]
        public void ScheduledCount_ReturnsActualValue()
        {
            // Arrange
            var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            var backgroundJobClient = new BackgroundJobClient(storage);

            // Act
            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello"),
                TimeSpan.FromSeconds(1));
            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello-2"),
                TimeSpan.FromSeconds(2));

            // Assert
            var scheduledCount = storage.GetMonitoringApi().ScheduledCount();
            Assert.Equal(2, scheduledCount);
        }

        [Fact, CleanDatabase]
        public void EnqueuedCount_ReturnsActualValue()
        {
            // Arrange
            var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            var backgroundJobClient = new BackgroundJobClient(storage);

            // Act
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));

            // Assert
            var enqueuedCount = storage.GetMonitoringApi().EnqueuedCount("default");
            Assert.Equal(3, enqueuedCount);
        }

        [Fact, CleanDatabase]
        public void FetchedCount_ReturnsActualValue()
        {
            // Arrange
            var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            var backgroundJobClient = new BackgroundJobClient(storage);

            // Act
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));

            var fetched = storage.GetConnection().FetchNextJob(new[] { "default" }, CancellationToken.None);

            // Assert
            var fetchedCount = storage.GetMonitoringApi().FetchedCount("default");
            Assert.Equal(1, fetchedCount);
        }

        [Fact, CleanDatabase]
        public void Queues_ReturnsActualQueues()
        {
            // Arrange
            var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            var backgroundJobClient = new BackgroundJobClient(storage);

            // Act
            backgroundJobClient.Create(() => Worker.DoWork("hello-1"), new EnqueuedState("default"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-2"), new EnqueuedState("test1"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-3"), new EnqueuedState("test2"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-4"), new EnqueuedState("default"));
            
            // Assert
            var queues = storage.GetMonitoringApi()
                .Queues()
                .Select(x => x.Name)
                .ToArray();

            Assert.Contains("default", queues);
            Assert.Contains("test1", queues);
            Assert.Contains("test2", queues);
        }
        
        public static class Worker
        {
            public static void DoWork(string argument)
            {
            }
        }
    }
}
