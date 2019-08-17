using System.Threading;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Regression
{
    public class RecurringJobFacts
    {
        // https://github.com/ahydrax/Hangfire.PostgreSql/issues/12
        [Fact, CleanDatabase]
        public void ParallelAddingSameRecurringJob_DoesntThrowException()
        {
            var storage = new PostgreSqlStorage(ConnectionUtils.GetConnectionString());
            var recurringJobManager = new RecurringJobManager(storage);

            ThreadStart action = () => recurringJobManager.AddOrUpdate("TEST_JOB", () => TestMethod(), Cron.Daily());

            var thread1 = new Thread(action);
            var thread2 = new Thread(action);

            thread1.Start();
            thread2.Start();

            thread1.Join();
            thread2.Join();
        }

        public static void TestMethod() { }
    }
}
