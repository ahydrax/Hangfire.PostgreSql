﻿using System.Collections.Generic;

namespace Hangfire.PostgreSql
{
    public interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();
        IEnumerable<int> GetEnqueuedJobIds(string queue, int from, int perPage);
        IEnumerable<int> GetFetchedJobIds(string queue, int from, int perPage);
        EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue);
    }
}
