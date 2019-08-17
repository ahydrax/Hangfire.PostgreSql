using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.PostgreSql.Queueing;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.PostgreSql.Tests.Integration
{
    public class JobQueueTest
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public JobQueueTest(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact, CleanDatabase]
        [Trait("Category", "Integration")]
        public void Throughput_Test()
        {
            var timeout = TimeSpan.FromSeconds(10);
            var source = new CancellationTokenSource(timeout);

            const int consumers = 10;
            const int producers = 10;

            int produced = 0;
            int consumed = 0;

            var queue = new JobQueue(
                ConnectionUtils.GetConnectionProvider(),
                new PostgreSqlStorageOptions { PrepareSchemaIfNecessary = false });

            var list = new List<Thread>();

            for (int i = 0; i < consumers; i++)
            {
                var thread = new Thread(() => Dequeue(queue, ref consumed, source.Token));
                list.Add(thread);
            }

            for (int i = 0; i < producers; i++)
            {
                var thread = new Thread(() => Enqueue(queue, ref produced, source.Token));
                list.Add(thread);
            }

            list.ForEach(x => x.Start());
            Thread.Sleep(timeout);
            list.ForEach(x => x.Join());

            _testOutputHelper.WriteLine($"produced: {produced}\nconsumed: {consumed}");
        }

        private void Enqueue(IJobQueue queue, ref int produced, CancellationToken ct)
        {
            var random = new Random();
            while (!ct.IsCancellationRequested)
            {
                queue.Enqueue("default", random.Next());
                Interlocked.Increment(ref produced);
            }
        }

        private void Dequeue(IJobQueue queue, ref int consumed, CancellationToken ct)
        {
            var queues = new string[] { "default" };
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var job = queue.Dequeue(queues, ct);
                    Interlocked.Increment(ref consumed);
                    job.RemoveFromQueue();
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
    }
}
