using System.Threading;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    internal interface IJobQueue
    {
        void Enqueue(string queue, long jobId);
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);
    }
}
