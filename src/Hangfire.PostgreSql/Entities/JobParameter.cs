using Hangfire.Annotations;

namespace Hangfire.PostgreSql.Entities
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    internal class JobParameter
    {
        public long JobId { get; set; }
        public string Name { get; set; }
        public string Value { get; set; }
    }
}
