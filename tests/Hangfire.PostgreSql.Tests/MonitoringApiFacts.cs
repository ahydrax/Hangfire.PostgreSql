using System;
using System.Linq;
using System.Threading;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class MonitoringApiFacts
    {
        private readonly IMonitoringApi _monitoringApi;
        private readonly PostgreSqlStorage _storage;

        public MonitoringApiFacts()
        {
            _storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            _monitoringApi = _storage.GetMonitoringApi();
        }

        [Fact, CleanDatabase]
        public void ScheduledCount_ReturnsActualValue()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);

            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello"),
                TimeSpan.FromSeconds(1));
            backgroundJobClient.Schedule(
                () => Worker.DoWork("hello-2"),
                TimeSpan.FromSeconds(2));

            // Act
            var scheduledCount = _storage.GetMonitoringApi().ScheduledCount();

            // Assert
            Assert.Equal(2, scheduledCount);
        }

        [Fact, CleanDatabase]
        public void EnqueuedCount_ReturnsActualValue()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));

            // Act
            var enqueuedCount = _monitoringApi.EnqueuedCount("default");

            // Assert
            Assert.Equal(3, enqueuedCount);
        }

        [Fact, CleanDatabase]
        public void FetchedCount_ReturnsActualValue()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-2"));
            backgroundJobClient.Enqueue(() => Worker.DoWork("hello-3"));
            var fetched = _storage.GetConnection().FetchNextJob(new[] { "default" }, CancellationToken.None);

            // Act
            var fetchedCount = _monitoringApi.FetchedCount("default");

            // Assert
            Assert.Equal(1, fetchedCount);
        }

        [Fact, CleanDatabase]
        public void Queues_ReturnsActualQueues()
        {
            // Arrange
            var backgroundJobClient = new BackgroundJobClient(_storage);
            backgroundJobClient.Create(() => Worker.DoWork("hello-1"), new EnqueuedState("default"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-2"), new EnqueuedState("test1"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-3"), new EnqueuedState("test2"));
            backgroundJobClient.Create(() => Worker.DoWork("hello-4"), new EnqueuedState("default"));

            // Act
            var queues = _monitoringApi
                .Queues()
                .Select(x => x.Name)
                .ToArray();

            // Assert
            Assert.Contains("default", queues);
            Assert.Contains("test1", queues);
            Assert.Contains("test2", queues);
        }

        [Fact, CleanDatabase]
        public void Servers_ReturnsActualServers()
        {
            // Arrange
            var queues = new[] { "default", "test" };
            var workerCount = 10;
            _storage.GetConnection().AnnounceServer("test-server", new ServerContext
            {
                Queues = queues,
                WorkerCount = workerCount
            });

            // Act
            var server = _monitoringApi
                .Servers()
                .Single();

            Assert.Equal("test-server", server.Name);
            Assert.Equal(queues, server.Queues);
            Assert.Equal(workerCount, server.WorkersCount);
        }

        public static class Worker
        {
            public static void DoWork(string argument)
            {
            }
        }
    }
}
