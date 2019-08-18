using System;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Maintenance;
using Hangfire.PostgreSql.Queueing;
using Hangfire.PostgreSql.Storage;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
    /// <summary>
    /// PostgreSQL storage implementation for Hangfire.
    /// </summary>
    [PublicAPI]
    public sealed class PostgreSqlStorage : JobStorage, IDisposable
    {
        private readonly PostgreSqlStorageOptions _options;
        private readonly IConnectionProvider _connectionProvider;
        private readonly string _storageInfo;
        private readonly StorageConnection _storageConnection;
        private readonly MonitoringApi _monitoringApi;
        private readonly PollingJobQueueProvider _queueProvider;

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection string and default PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> is not valid PostgreSql connection string </exception>
        public PostgreSqlStorage(string connectionString)
            : this(connectionString, new PostgreSqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection string and the provided PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionString">A Postgres connection string</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionString"/> is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionString"/> is not valid PostgreSql connection string.</exception>
        public PostgreSqlStorage(string connectionString, PostgreSqlStorageOptions options)
            : this(new DefaultConnectionBuilder(connectionString), options)
        {

        }

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection builder and default PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionBuilder">A Postgres connection builder</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionBuilder"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionBuilder"/> is not valid PostgreSql connection string </exception>
        public PostgreSqlStorage(IConnectionBuilder connectionBuilder)
            : this(connectionBuilder, new PostgreSqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes PostgreSqlStorage with the provided connection builder and the provided PostgreSqlStorageOptions.
        /// </summary>
        /// <param name="connectionBuilder">PostgreSQL connection builder.</param>
        /// <param name="options">PostgreSQL storage options.</param>
        /// <exception cref="ArgumentNullException"><paramref name="connectionBuilder"/> is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="connectionBuilder"/> has an invalid PostgreSql connection string.</exception>
        public PostgreSqlStorage(IConnectionBuilder connectionBuilder, PostgreSqlStorageOptions options)
        {
            Guard.ThrowIfNull(connectionBuilder, nameof(connectionBuilder));
            Guard.ThrowIfNull(options, nameof(options));
            Guard.ThrowIfConnectionStringIsInvalid(connectionBuilder.ConnectionStringBuilder.ConnectionString);

            _options = options;

            _connectionProvider = CreateConnectionProvider(connectionBuilder);

            var queue = new JobQueue(_connectionProvider, _options);
            _queueProvider = new PollingJobQueueProvider(_connectionProvider, TimeSpan.FromMinutes(1));
            _storageConnection = new StorageConnection(_connectionProvider, queue, _options);
            _monitoringApi = new MonitoringApi(_connectionProvider, _queueProvider);

            var builder = connectionBuilder.ConnectionStringBuilder;
            _storageInfo = $"PostgreSQL Server: Host: {builder.Host}, DB: {builder.Database}, Schema: {builder.SearchPath}, Pool: {_connectionProvider.GetType().Name}";

            PrepareSchemaIfNecessary(builder.SearchPath);
        }

        private static IConnectionProvider CreateConnectionProvider(IConnectionBuilder connectionBuilder)
        {
            return connectionBuilder.ConnectionStringBuilder.Pooling
                ? new NpgsqlConnectionProvider(connectionBuilder)
                : (IConnectionProvider)new DefaultConnectionProvider(connectionBuilder);
        }

        private void PrepareSchemaIfNecessary(string schemaName)
        {
            if (_options.PrepareSchemaIfNecessary)
            {
                new DatabaseInitializer(_connectionProvider, schemaName).Initialize();
            }
        }

        internal IConnectionProvider ConnectionProvider => _connectionProvider;

        /// <inheritdoc/>
        public override IMonitoringApi GetMonitoringApi() => _monitoringApi;

        /// <inheritdoc/>
        public override IStorageConnection GetConnection() => _storageConnection;

#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
        /// <inheritdoc/>
        public override IEnumerable<IServerComponent> GetComponents()
            => new IServerComponent[]
#pragma warning restore 618
            {
                _queueProvider,
                new ExpirationManager(_connectionProvider),
                new ExpiredLocksManager(_connectionProvider, _options.DistributedLockTimeout),
                new CountersAggregationManager(_connectionProvider)
            };

        /// <inheritdoc/>
        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.Info(_storageInfo);
            logger.InfoFormat("  Prepare schema: {0}.", _options.PrepareSchemaIfNecessary);
            logger.InfoFormat("  Queue poll interval: {0}.", _options.QueuePollInterval);
            logger.InfoFormat("  Invisibility timeout: {0}.", _options.InvisibilityTimeout);
            logger.InfoFormat("  Distributed lock timeout: {0}.", _options.DistributedLockTimeout);
        }

        /// <inheritdoc/>
        public override string ToString() => _storageInfo;

        /// <inheritdoc/>
        public void Dispose() => _connectionProvider.Dispose();
    }
}
