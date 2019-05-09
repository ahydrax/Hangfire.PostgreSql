using System;
using Hangfire.Annotations;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class SqlState
    {
        public long JobId { get; set; }
        public string Name { get; set; }
        public string Reason { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Data { get; set; }
    }
}
