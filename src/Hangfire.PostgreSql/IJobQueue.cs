using System.Collections.Generic;
using System.Threading;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.PostgreSql
{
    public interface IJobQueue
    {
        void Enqueue(string queue, string jobId);
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
    }

    public interface IExtendedJobQueue : IJobQueue
    {
        IList<QueueWithTopEnqueuedJobsDto> Queues();
        long EnqueuedCount(string queue);
        long FetchedCount(string queue);
        IEnumerable<string> GetQueues();
    }
}
