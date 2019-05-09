using System;
using Hangfire.Annotations;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class SqlHash
    {
        public long Id { get; set; }
        public string Key { get; set; }
        public string Field { get; set; }
        public string Value { get; set; }
        public DateTime? ExpireAt { get; set; }
    }
}
