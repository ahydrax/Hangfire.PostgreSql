namespace Hangfire.PostgreSql.Entities
{
    internal struct Option<T> where T : class
    {
        public readonly T Value;
        public bool HasValue => Value != null;

        public Option(T value)
        {
            Value = value;
        }
    }
}
