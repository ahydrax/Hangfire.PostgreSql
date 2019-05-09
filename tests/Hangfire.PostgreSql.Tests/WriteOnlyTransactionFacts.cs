using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using Dapper;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.States;
using Moq;
using Npgsql;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class WriteOnlyTransactionFacts
    {
        private readonly Mock<IJobQueue> _queue;

        public WriteOnlyTransactionFacts()
        {
            _queue = new Mock<IJobQueue>();
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new WriteOnlyTransaction(null, _queue.Object));

            Assert.Equal("connectionProvider", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfQueueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new WriteOnlyTransaction(ConnectionUtils.GetConnectionProvider(), null));

            Assert.Equal("queue", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job""(""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnection((provider, connection) =>
            {
                var jobId = connection.Query(arrangeSql).Single().id.ToString();
                var anotherJobId = connection.Query(arrangeSql).Single().id.ToString();

                Commit(provider, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var job = GetTestJob(connection, jobId);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < job.expireat &&
                            job.expireat <= DateTime.UtcNow.AddDays(1));

                var anotherJob = GetTestJob(connection, anotherJobId);
                Assert.Null(anotherJob.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"", ""expireat"")
values ('', '', now() at time zone 'utc', now() at time zone 'utc') returning ""id""";


            UseConnection((provider, connection) =>
            {
                var jobId = connection.Query(arrangeSql).Single().id.ToString();
                var anotherJobId = connection.Query(arrangeSql).Single().id.ToString();

                Commit(provider, x => x.PersistJob(jobId));

                var job = GetTestJob(connection, jobId);
                Assert.Null(job.expireat);

                var anotherJob = GetTestJob(connection, anotherJobId);
                Assert.NotNull(anotherJob.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc') returning ""id""";

            UseConnection((provider, connection) =>
            {
                var jobId = connection.Query(arrangeSql).Single().id.ToString();
                var anotherJobId = connection.Query(arrangeSql).Single().id.ToString();

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(provider, x => x.SetJobState(jobId, state.Object));

                var job = GetTestJob(connection, jobId);
                Assert.Equal("State", job.statename);
                Assert.NotNull(job.stateid);

                var anotherJob = GetTestJob(connection, anotherJobId);
                Assert.Null(anotherJob.statename);
                Assert.Null(anotherJob.stateid);

                var jobState = connection.Query(@"select * from """ + GetSchemaName() + @""".""state""").Single();
                Assert.Equal((string)jobId, jobState.jobid.ToString());
                Assert.Equal("State", jobState.name);
                Assert.Equal("Reason", jobState.reason);
                Assert.NotNull(jobState.createdat);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.data);
            });
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            string arrangeSql = @"
insert into """ + GetSchemaName() + @""".""job"" (""invocationdata"", ""arguments"", ""createdat"")
values ('', '', now() at time zone 'utc')
returning ""id""";

            UseConnection((provider, connection) =>
            {
                var jobId = connection.Query(arrangeSql).Single().id.ToString(CultureInfo.InvariantCulture);

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(provider, x => x.AddJobState(jobId, state.Object));

                var job = GetTestJob(connection, jobId);
                Assert.Null(job.StateName);
                Assert.Null(job.StateId);

                var jobState = connection.Query(@"select * from """ + GetSchemaName() + @""".""state""").Single();
                Assert.Equal((string)jobId, jobState.jobid.ToString(CultureInfo.InvariantCulture));
                Assert.Equal("State", jobState.name);
                Assert.Equal("Reason", jobState.reason);
                Assert.NotNull(jobState.createdat);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.data);
            });
        }

        [Fact, CleanDatabase]
        public void AddToQueue_InsertsJobIdToQueue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.AddToQueue("default", "1"));

                var queueLength = connection.ExecuteScalar<int>("SELECT count(*) FROM jobqueue WHERE jobId = 1");

                Assert.Equal(1, queueLength);
            });
        }

        private static dynamic GetTestJob(IDbConnection connection, string jobId)
        {
            return connection
                .Query(@"SELECT * FROM job WHERE id = @id", new { id = JobId.ToLong(jobId) })
                .Single();
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.IncrementCounter("my-key"));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(1, record.value);
                Assert.Equal((DateTime?)null, record.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(1, record.value);
                Assert.NotNull(record.expireat);

                var expireAt = (DateTime)record.expireat;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""")
                    .Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.DecrementCounter("my-key"));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(-1, record.value);
                Assert.Equal((DateTime?)null, record.expireat);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""counter""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal(-1, record.value);
                Assert.NotNull(record.expireat);

                var expireAt = (DateTime)record.expireat;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""counter""")
                    .Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.AddToSet("my-key", "my-value"));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal("my-value", record.value);
                Assert.Equal(0.0, record.score, 2);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""")
                    .Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""")
                    .Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.AddToSet("my-key", "my-value", 3.2));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal("my-value", record.value);
                Assert.Equal(3.2, record.score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();

                Assert.Equal(3.2, record.score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""")
                    .Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""")
                    .Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""set""")
                    .Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.InsertToList("my-key", "my-value"));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""list""").Single();

                Assert.Equal("my-key", record.key);
                Assert.Equal("my-value", record.value);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".""list""").ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].value);
                Assert.Equal("2", records[1].value);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""list""")
                    .Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection((provider, connection) =>
            {
                Commit(provider, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                }));

                var result = connection.Query(
                        @"select * from """ + GetSchemaName() + @""".""hash"" where ""key"" = @key",
                        new { key = "some-hash" })
                    .ToDictionary(x => (string)x.field, x => (string)x.value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection((provider, connection) =>
            {
                // Arrange
                Commit(provider, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                }));

                // Act
                Commit(provider, x => x.RemoveHash("some-hash"));

                // Assert
                var count = connection.Query<long>(@"select count(*) from """ + GetSchemaName() + @""".""hash""").Single();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.AddRangeToSet(null, new List<string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.AddRangeToSet("my-set", null)));

                Assert.Equal("items", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_AddsAllItems_ToAGivenSet()
        {
            UseConnection((provider, connection) =>
            {
                var items = new List<string> { "1", "2", "3" };

                Commit(provider, x => x.AddRangeToSet("my-set", items));

                var records = connection.Query<string>(@"select ""value"" from """ + GetSchemaName() +
                                                @""".""set"" where ""key"" = 'my-set'");
                Assert.Equal(items, records);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.RemoveSet(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveSet_RemovesASet_WithAGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() +
                                @""".""set"" (""key"", ""value"", ""score"") values (@key, @value, 0.0)";

            UseConnection((provider, connection) =>
            {
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "set-1", value = "1"},
                    new {key = "set-2", value = "1"}
                });

                Commit(provider, x => x.RemoveSet("set-1"));

                var record = connection.Query(@"select * from """ + GetSchemaName() + @""".""set""").Single();
                Assert.Equal("set-2", record.key);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.ExpireHash(null, TimeSpan.FromMinutes(5))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() +
                                @""".hash (""key"", ""field"") values (@key, @field)";

            UseConnection((provider, connection) =>
            {
                // Arrange
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "hash-1", field = "field"},
                    new {key = "hash-2", field = "field"}
                });

                // Act
                Commit(provider, x => x.ExpireHash("hash-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".hash")
                    .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["hash-1"]);
                Assert.True(records["hash-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["hash-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.ExpireSet(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() +
                                @""".""set"" (""key"", ""value"", ""score"") values (@key, @value, 0.0)";

            UseConnection((provider, connection) =>
            {
                // Arrange
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "set-1", value = "1"},
                    new {key = "set-2", value = "1"}
                });

                // Act
                Commit(provider, x => x.ExpireSet("set-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".""set""")
                    .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
                Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["set-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.ExpireList(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() + @""".""list"" (""key"") values (@key)";

            UseConnection((provider, connection) =>
            {
                // Arrange
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "list-1", value = "1"},
                    new {key = "list-2", value = "1"}
                });

                // Act
                Commit(provider, x => x.ExpireList("list-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".""list""")
                    .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
                Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["list-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.PersistHash(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ClearsExpirationTime_OnAGivenHash()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() +
                                @""".hash (""key"", ""field"", ""expireat"") values (@key, @field, @expireAt)";

            UseConnection((provider, connection) =>
            {
                // Arrange
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "hash-1", field = "field", expireAt = DateTime.UtcNow.AddDays(1)},
                    new {key = "hash-2", field = "field", expireAt = DateTime.UtcNow.AddDays(1)}
                });

                // Act
                Commit(provider, x => x.PersistHash("hash-1"));

                // Assert
                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".hash")
                    .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.Null(records["hash-1"]);
                Assert.NotNull(records["hash-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.PersistSet(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistSet_ClearsExpirationTime_OnAGivenHash()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() +
                                @""".""set"" (""key"", ""value"", ""expireat"", ""score"") values (@key, @value, @expireAt, 0.0)";

            UseConnection((provider, connection) =>
            {
                // Arrange
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "set-1", value = "1", expireAt = DateTime.UtcNow.AddDays(1)},
                    new {key = "set-2", value = "1", expireAt = DateTime.UtcNow.AddDays(1)}
                });

                // Act
                Commit(provider, x => x.PersistSet("set-1"));

                // Assert
                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".""set""")
                    .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.Null(records["set-1"]);
                Assert.NotNull(records["set-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((provider, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(provider, x => x.PersistList(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ClearsExpirationTime_OnAGivenHash()
        {
            string arrangeSql = @"insert into """ + GetSchemaName() +
                                @""".""list"" (""key"", ""expireat"") values (@key, @expireAt)";

            UseConnection((provider, connection) =>
            {
                // Arrange
                connection.Execute(arrangeSql, new[]
                {
                    new {key = "list-1", expireAt = DateTime.UtcNow.AddDays(1)},
                    new {key = "list-2", expireAt = DateTime.UtcNow.AddDays(1)}
                });

                // Act
                Commit(provider, x => x.PersistList("list-1"));

                // Assert
                var records = connection.Query(@"select * from """ + GetSchemaName() + @""".""list""")
                    .ToDictionary(x => (string)x.key, x => (DateTime?)x.expireat);
                Assert.Null(records["list-1"]);
                Assert.NotNull(records["list-2"]);
            });
        }

        private void UseConnection(Action<IConnectionProvider, NpgsqlConnection> action)
        {
            var provider = ConnectionUtils.GetConnectionProvider();
            using (var connection = provider.AcquireConnection())
            {
                action(provider, connection.Connection);
            }
        }

        private void Commit(IConnectionProvider provider, Action<WriteOnlyTransaction> action)
        {
            using (var transaction = new WriteOnlyTransaction(provider, _queue.Object))
            {
                action(transaction);
                transaction.Commit();
            }
        }

        private static string GetSchemaName()
        {
            return ConnectionUtils.GetSchemaName();
        }
    }
}
