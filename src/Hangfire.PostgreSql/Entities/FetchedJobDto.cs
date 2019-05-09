using System;
using Hangfire.Annotations;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class FetchedJobDto
    {
        public long Id { get; set; }
        public long JobId { get; set; }
        public string Queue { get; set; }
        public DateTime? FetchedAt { get; set; }
    }
}
