﻿using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.PostgreSql.Tests.Web
{
    public static class TestSuite
    {
        struct DummyStruct
        {
            public int A;
            public int B;
            public int C;
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
            Parallel.For(0, 1, i =>
            {
                var w1 = Stopwatch.StartNew();
                Stopwatch watch = new Stopwatch();
                watch.Start();

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
            var jobA = BackgroundJob.Enqueue(() => ContinuationPartA());
            var jobB = BackgroundJob.ContinueWith(jobA, () => ContinuationPartB(), JobContinuationOptions.OnlyOnSucceededState);
            BackgroundJob.ContinueWith(jobB, () => ContinuationPartC(), JobContinuationOptions.OnAnyFinishedState);
        }

        [Queue("queue1")]
        public static void ContinuationPartA()
        {
            Thread.Sleep(TimeSpan.FromSeconds(5));
        }

        [AutomaticRetry(Attempts = 0)]
        public static void ContinuationPartB()
        {
            throw new InvalidOperationException("TEST OK");
        }
        
        [Queue("queue2")]
        public static string ContinuationPartC()
        {
            return "DONE";
        }
    }
}
