using Hangfire.Annotations;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class EnqueuedAndFetchedJobsCount
    {
        public long Enqueued { get; set; }
        public long Fetched { get; set; }
    }
}
