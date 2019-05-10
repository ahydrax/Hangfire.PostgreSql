using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Hangfire.Storage;
using Xunit;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class DistributedLockTest
    {
        [Fact, CleanDatabase]
        [Trait("Category","Integration")]
        public static void Perf_AcquiringLock_SameResource()
        {
            var connectionProvider = ConnectionUtils.GetConnectionProvider();
            var lockTimeout = TimeSpan.FromMilliseconds(1000);
            const int concurrentQueries = 1000;
            
            var threads = RunConcurrent(concurrentQueries,
                lockTimeout,
                connectionProvider,
                x => "oneLockToRuleThemAll");

            Assert.Equal(concurrentQueries, threads);
        }
        
        [Fact, CleanDatabase]
        [Trait("Category","Integration")]
        public static void Perf_AcquiringLock_DifferentResources()
        {
            var connectionProvider = ConnectionUtils.GetConnectionProvider();
            var lockTimeout = TimeSpan.Zero;
            const int concurrentQueries = 1000;

            var successFullThreads = RunConcurrent(concurrentQueries,
                lockTimeout,
                connectionProvider,
                x => x.ToString());

            Assert.Equal(concurrentQueries, successFullThreads);
        }

        private static int RunConcurrent(int concurrentQueries,
            TimeSpan lockTimeout,
            IConnectionProvider connectionProvider,
            Func<int, string> lockNameSelector)
        {
            return Enumerable.Range(1, concurrentQueries)
                .AsParallel()
                .WithDegreeOfParallelism(Math.Min(concurrentQueries, 511)) // 511 is max for that method
                .WithExecutionMode(ParallelExecutionMode.ForceParallelism)
                .AsUnordered()
                .Select(x => AcquireLock(lockNameSelector(x), lockTimeout, connectionProvider))
                .Sum();
        }

        private static int AcquireLock(string resourceName, TimeSpan lockTimeout, IConnectionProvider connectionProvider)
        {
            try
            {
                using (new DistributedLock(resourceName, lockTimeout, connectionProvider))
                {
                    return 1;
                }
            }
            catch (DistributedLockTimeoutException)
            {
                return 0;
            }
        }

        [Fact, CleanDatabase]
        [Trait("Category","Integration")]
        public static void Ctor_ActuallyGrantsExclusiveLock()
        {
            const int numberOfParallelJobs = 1000;
            var connectionProvider = ConnectionUtils.GetConnectionProvider();
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = numberOfParallelJobs };
            var i = 0;

            Parallel.For(0, numberOfParallelJobs, parallelOptions, _ =>
            {
                using (new DistributedLock("increment_test", TimeSpan.FromSeconds(1), connectionProvider))
                {
                    // prevent compiler/jit from reordering
                    var temp = Volatile.Read(ref i);
                    Volatile.Write(ref i, temp + 1);
                }
            });

            Assert.Equal(numberOfParallelJobs, i);
        }
    }
}
