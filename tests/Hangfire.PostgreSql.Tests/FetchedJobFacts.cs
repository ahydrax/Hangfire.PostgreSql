using System;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class FetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        private readonly Mock<IConnectionProvider> _connection;

        public FetchedJobFacts()
        {
            _connection = new Mock<IConnectionProvider>();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FetchedJob(null, 1, JobId, Queue));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FetchedJob(_connection.Object, 1, null, Queue));

            Assert.Equal("jobId", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new FetchedJob(_connection.Object, 1, JobId, null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            var fetchedJob = new FetchedJob(_connection.Object, 1, JobId, Queue);

            Assert.Equal(1, fetchedJob.Id);
            Assert.Equal(JobId, fetchedJob.JobId);
            Assert.Equal(Queue, fetchedJob.Queue);
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, "1", "default");
                var processingJob = new FetchedJob(provider, id, "1", "default");

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""jobqueue""")
                    .Single();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                CreateJobQueueRecord(connection, "1", "default");
                CreateJobQueueRecord(connection, "1", "critical");
                CreateJobQueueRecord(connection, "2", "default");

                var fetchedJob = new FetchedJob(provider, 999, "1", "default");

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                var count = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""jobqueue""")
                    .Single();
                Assert.Equal(3, count);
            });
        }

        [Fact, CleanDatabase]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, "1", "default");
                var processingJob = new FetchedJob(provider, id, "1", "default");

                // Act
                processingJob.Requeue();

                // Assert
                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                var id = CreateJobQueueRecord(connection, "1", "default");
                var processingJob = new FetchedJob(provider, id, "1", "default");

                // Act
                processingJob.Dispose();

                // Assert
                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
                Assert.Null(record.fetchedat);
            });
        }

        private static int CreateJobQueueRecord(IDbConnection connection, string jobId, string queue)
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
values (@id, @queue, now() at time zone 'utc') returning ""id""";

            return
                (int)
                connection.Query(arrangeSql,
                        new { id = PostgreSql.JobId.ToLong(jobId), queue = queue })
                    .Single()
                    .id;
        }

        private static void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            var connectionProvider = ConnectionUtils.GetConnectionProvider();

            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                action(connectionProvider, connectionHolder.Connection);
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}
