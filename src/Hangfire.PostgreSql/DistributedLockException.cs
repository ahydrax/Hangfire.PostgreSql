using System;
using System.Runtime.Serialization;
using Hangfire.Annotations;

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

        /// <inheritdoc/>
        protected DistributedLockException([NotNull] SerializationInfo info, StreamingContext context)
            : base(info, context) { }
    }
}
