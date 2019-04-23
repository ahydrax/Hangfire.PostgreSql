﻿using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class JobQueueFacts
    {
        private static readonly string[] DefaultQueues = { "default" };

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new JobQueue(null, new PostgreSqlStorageOptions()));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new JobQueue(new Mock<IConnectionProvider>().Object, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue();

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromQueueWithHigherPriority()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue();
                var token = CreateTimingOutCancellationToken();

                queue.Enqueue("1", 1);
                queue.Enqueue("2", 2);
                queue.Enqueue("3", 3);

                Assert.Equal("1", queue.Dequeue(new[] { "1", "2", "3" }, token).JobId);
                Assert.Equal("2", queue.Dequeue(new[] { "2", "3", "1" }, token).JobId);
                Assert.Equal("3", queue.Dequeue(new[] { "3", "1", "2" }, token).JobId);
            });
        }

        [Fact, CleanDatabase]
        private void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue();

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        private void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue();

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue();

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
values (@jobId, @queue) returning ""id""";

            // Arrange
            UseConnection(connection =>
            {
                var id = (int)connection.Query(
                    arrangeSql,
                    new { jobId = 1, queue = "default" }).Single().id;
                var queue = CreateJobQueue();

                // Act
                var payload = (FetchedJob)queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.Equal(id, payload.Id);
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";

            // Arrange
            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new { invocationData = "", arguments = "", queue = "default" });
                var queue = CreateJobQueue();

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = connection.Query<DateTime?>(
                    @"select ""fetchedat"" from """ + GetSchemaName() + @""".""jobqueue"" where ""jobid"" = @id",
                    new { id = Convert.ToInt32(payload.JobId, CultureInfo.InvariantCulture) }).Single();

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"", ""fetchedat"")
select i.""id"", @queue, @fetchedAt from i;
";

            // Arrange
            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new
                    {
                        queue = "default",
                        fetchedAt = DateTime.UtcNow.AddDays(-1),
                        invocationData = "",
                        arguments = ""
                    });
                var queue = CreateJobQueue();

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";

            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new {queue = "default", invocationData = "", arguments = ""},
                        new {queue = "default", invocationData = "", arguments = ""}
                    });
                var queue = CreateJobQueue();

                // Act
                var payload = queue.Dequeue(
                    DefaultQueues,
                    CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = connection.Query<DateTime?>(
                    @"select ""fetchedat"" from """ + GetSchemaName() + @""".""jobqueue"" where ""jobid"" <> @id",
                    new { id = Convert.ToInt32(payload.JobId, CultureInfo.InvariantCulture) }).Single();

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
RETURNING ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";
            UseConnection(connection =>
            {
                var queue = CreateJobQueue();

                connection.Execute(
                    arrangeSql,
                    new { queue = "critical", invocationData = "", arguments = "" });

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken()));
            });
        }

        [Fact, CleanDatabase]
        private void Dequeue_ShouldFetchJobs_FromMultipleQueues()
        {
            string arrangeSql = @"
WITH i AS (
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values (@invocationData, @arguments, now() at time zone 'utc')
returning ""id"")
insert into """ + GetSchemaName() + @""".""jobqueue"" (""jobid"", ""queue"")
select i.""id"", @queue from i;
";

            var queueNames = new[] { "default", "critical" };

            UseConnection(connection =>
            {
                connection.Execute(
                    arrangeSql,
                    new[]
                    {
                        new {queue = queueNames.First(), invocationData = "", arguments = ""},
                        new {queue = queueNames.Last(), invocationData = "", arguments = ""}
                    });

                var queue = CreateJobQueue();

                var queueFirst = (FetchedJob)queue.Dequeue(
                    queueNames,
                    CreateTimingOutCancellationToken());

                Assert.NotNull(queueFirst.JobId);
                Assert.Contains(queueFirst.Queue, queueNames);

                var queueLast = (FetchedJob)queue.Dequeue(
                    queueNames,
                    CreateTimingOutCancellationToken());

                Assert.NotNull(queueLast.JobId);
                Assert.Contains(queueLast.Queue, queueNames);
            });
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseConnection(connection =>
            {
                var queue = CreateJobQueue();

                queue.Enqueue("default", 1);

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""jobqueue""").Single();
                Assert.Equal("1", record.jobid.ToString());
                Assert.Equal("default", record.queue);
                Assert.Null(record.FetchedAt);
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            return source.Token;
        }

        private static JobQueue CreateJobQueue()
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            return new JobQueue(provider, new PostgreSqlStorageOptions());
        }

        private static void UseConnection(Action<NpgsqlConnection> connectionSetup)
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = provider.AcquireConnection())
            {
                connectionSetup(connection.Connection);
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}
