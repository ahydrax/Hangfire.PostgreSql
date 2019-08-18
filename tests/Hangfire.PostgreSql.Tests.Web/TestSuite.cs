using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Console;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Tests.Web
{
    public static class TestSuite
    {
        struct DummyStruct
        {
            public int A;
#pragma warning disable 649 // used only for payload
            public int B;
            public int C;
#pragma warning restore 649
        }

        public static void Alloc()
        {
            var w1 = Stopwatch.StartNew();
            var rnd = new Random();
            while (true)
            {
                var alloc = rnd.Next(100000, 10000000);
                var x = new DummyStruct[alloc];
                x[0].A = 1;
                x[x.Length - 1].A = 2;
                if (w1.Elapsed > TimeSpan.FromSeconds(15))
                {
                    break;
                }
            }
        }

        public static void CpuKill(int cpuUsage)
        {
            Parallel.For(0, 1, new ParallelOptions { MaxDegreeOfParallelism = 100 }, i =>
              {
                  var w1 = Stopwatch.StartNew();
                  var watch = Stopwatch.StartNew();
                  while (true)
                  {
                      if (watch.ElapsedMilliseconds > cpuUsage)
                      {
                          Thread.Sleep(100 - cpuUsage);
                          watch.Reset();
                          watch.Start();
                      }

                      if (w1.Elapsed > TimeSpan.FromSeconds(15))
                      {
                          break;
                      }
                  }
              });
        }

        public static void ContinuationTest()
        {
            var jobA = BackgroundJob.Enqueue(() => ContinuationPartA(null));
            var jobB = BackgroundJob.ContinueJobWith(jobA,
                () => ContinuationPartB(),
                JobContinuationOptions.OnlyOnSucceededState);

            BackgroundJob.ContinueJobWith(jobB,
                () => ContinuationPartC(),
                JobContinuationOptions.OnAnyFinishedState);
        }

        [Queue("queue1")]
        public static async Task ContinuationPartA(PerformContext context)
        {
            var progress = context.WriteProgressBar("completion", 0);
            await Task.Yield();

            for (int i = 0; i < 100; i++)
            {
                await Task.Delay(100);
                progress.SetValue(i + 1);
            }

            for (int i = 0; i < 1000; i++)
            {
                context.WriteLine($"printed {i}, {DateTime.UtcNow:O}");
            }

            await Task.Delay(5000);
        }

        [AutomaticRetry(Attempts = 0)]
        public static void ContinuationPartB() => throw new InvalidOperationException("TEST OK");

        [Queue("queue2")]
        public static string ContinuationPartC() => "DONE";

        [Queue("queue1")]
        public static string ContinuationPartC2() => "DONE";

        [Queue("default")]
        public static string ContinuationPartC3() => "DONE";

        public static string ContinuationPartC4() => "DONE";

        public static object TaskBurst()
        {
            var bjc = new BackgroundJobClient(JobStorage.Current);

            const int tasksPerBatch = 1000;
            const int parallelRuns = 50;

            Parallel.For(0, parallelRuns, new ParallelOptions { MaxDegreeOfParallelism = parallelRuns }, i =>
            {
                for (int j = 0; j < tasksPerBatch; j++)
                {
                    if (i % 2 == 0)
                    {
                        bjc.Enqueue(() => ContinuationPartC2());
                    }
                    else if (i % 3 == 0)
                    {
                        bjc.Enqueue(() => ContinuationPartC3());
                    }
                    else
                    {
                        bjc.Enqueue(() => ContinuationPartC());
                    }
                }
            });

            return new
            {
                created = tasksPerBatch * parallelRuns
            };
        }
    }
}
