using System;
using Hangfire.Annotations;
using Hangfire.PostgreSql.Connectivity;
using Npgsql;

namespace Hangfire.PostgreSql
{
    /// <summary>
    /// Contains extension methods for configuring storage.
    /// </summary>
    public static class PostgreSqlBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage that can be accessed
        /// using the given connection string.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            string connectionString)
        {
            var storage = new PostgreSqlStorage(connectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage that can be accessed
        /// using a connection from the given connection builder.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionBuilder">Connection builder</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            IConnectionBuilder connectionBuilder)
        {
            var storage = new PostgreSqlStorage(connectionBuilder);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage that can be accessed
        /// using a connection from the given connection builder.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionStringBuilder">Connection string builder</param>
        /// <param name="connectionSetup">Optional setup action to apply to connections created using <paramref name="connectionStringBuilder"/></param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration, 
            NpgsqlConnectionStringBuilder connectionStringBuilder,
            Action<NpgsqlConnection> connectionSetup = null)
        {
            var connectionBuilder = new DefaultConnectionBuilder(connectionStringBuilder);
            var storage = new PostgreSqlStorage(connectionBuilder);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage with the given options,
        /// that can be accessed using the specified connection string.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="options">Advanced options</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            string connectionString,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(connectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage with the given options,
        /// that can be accessed using a connection from the specified connection builder.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionBuilder">Connection builder</param>
        /// <param name="options">Advanced options</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this GlobalConfiguration configuration,
            IConnectionBuilder connectionBuilder,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(connectionBuilder, options);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage, that can be accessed
        /// using the given connection string.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            string connectionString)
        {
            var storage = new PostgreSqlStorage(connectionString);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage, that can be accessed
        /// using a connection from the given connection builder.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionBuilder">Connection builder</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            IConnectionBuilder connectionBuilder)
        {
            var storage = new PostgreSqlStorage(connectionBuilder);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage with the given options,
        /// that can be accessed using the specified connection string or its name.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string</param>
        /// <param name="options">Advanced options</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            string connectionString,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(connectionString, options);
            configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use PostgreSQL as a job storage with the given options,
        /// that can be accessed using a connection from the specified connection builder.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionBuilder">Connection builder</param>
        /// <param name="options">Advanced options</param>
        [PublicAPI]
        public static PostgreSqlStorage UsePostgreSqlStorage(
            this IGlobalConfiguration configuration,
            IConnectionBuilder connectionBuilder,
            PostgreSqlStorageOptions options)
        {
            var storage = new PostgreSqlStorage(connectionBuilder, options);
            configuration.UseStorage(storage);

            return storage;
        }
    }
}
