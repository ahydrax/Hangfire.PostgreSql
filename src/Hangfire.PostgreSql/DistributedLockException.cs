using System;

namespace Hangfire.PostgreSql
{
    /// <summary>
    /// Occurs when distributed lock cannot be released.
    /// </summary>
    [Serializable]
    public class DistributedLockException : Exception
    {
        /// <inheritdoc/>
        public DistributedLockException(string message)
            : base(message) { }
    }
}
