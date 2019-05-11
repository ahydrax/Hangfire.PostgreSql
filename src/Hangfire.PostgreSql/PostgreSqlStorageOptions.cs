using System;

namespace Hangfire.PostgreSql
{
    /// <summary>
    /// Contains PostgreSQL storage options
    /// </summary>
    public class PostgreSqlStorageOptions
    {
        private TimeSpan _queuePollInterval;
        private TimeSpan _invisibilityTimeout;
        private TimeSpan _distributedLockTimeout;

        /// <summary>
        /// Initializes options with default values.
        /// </summary>
        public PostgreSqlStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromMilliseconds(300);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            DistributedLockTimeout = TimeSpan.FromMinutes(10);
            PrepareSchemaIfNecessary = true;
        }

        /// <summary>
        /// Gets or sets a pause length between queue querying.
        /// </summary>
        public TimeSpan QueuePollInterval
        {
            get => _queuePollInterval;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(QueuePollInterval));
                _queuePollInterval = value;
            }
        }

        /// <summary>
        /// Gets or sets job invisibility timeout in queue.
        /// </summary>
        public TimeSpan InvisibilityTimeout
        {
            get => _invisibilityTimeout;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(InvisibilityTimeout));
                _invisibilityTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets timeout for distributed locks.
        /// </summary>
        public TimeSpan DistributedLockTimeout
        {
            get => _distributedLockTimeout;
            set
            {
                Guard.ThrowIfValueIsNotPositive(value, nameof(DistributedLockTimeout));
                _distributedLockTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets whether storage should handle database schema.
        /// </summary>
        public bool PrepareSchemaIfNecessary { get; set; }
    }
}
