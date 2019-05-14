using System;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    /// <summary>
    /// Default implementation for connection bootstrapper.
    /// </summary>
    public class DefaultConnectionBuilder : IConnectionBuilder
    {
        private readonly Action<NpgsqlConnection> _connectionSetup;

        /// <inheritdoc />
        public NpgsqlConnectionStringBuilder ConnectionStringBuilder { get; }

        /// <inheritdoc />
        /// <summary>
        /// Create a new connection builder using the supplied connection string and connection
        /// setup action
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
        public DefaultConnectionBuilder(string connectionString, 
            Action<NpgsqlConnection> connectionSetup = null) 
            : this(new NpgsqlConnectionStringBuilder(connectionString), connectionSetup)
        {
            Guard.ThrowIfNull(connectionString, nameof(connectionString));
            Guard.ThrowIfConnectionStringIsInvalid(connectionString);
        }

        /// <summary>
        /// Create a new connection builder using the supplied connection string builder and
        /// connection setup action
        /// </summary>
        /// <param name="connectionStringBuilder">Connection string builder</param>
        /// <param name="connectionSetup">Optional setup action to apply to created connections</param>
        public DefaultConnectionBuilder(NpgsqlConnectionStringBuilder connectionStringBuilder, 
            Action<NpgsqlConnection> connectionSetup = null)
        {
            Guard.ThrowIfNull(connectionStringBuilder, nameof(connectionStringBuilder));

            ConnectionStringBuilder = connectionStringBuilder;
            _connectionSetup = connectionSetup;
        }

        /// <inheritdoc />
        /// <summary>
        /// Create a new connection. If the <see cref="T:Hangfire.PostgreSql.Connectivity.DefaultConnectionBuilder" /> was created
        /// with a connection setup action, it will be applied to the connection before it is
        /// returned
        /// </summary>
        /// <returns>A new connection, optionally setup using a connection setup action</returns>
        public NpgsqlConnection Build()
        {
            var connection = new NpgsqlConnection(ConnectionStringBuilder.ConnectionString);

            // Setup the connection, if required
            _connectionSetup?.Invoke(connection);

            return connection;
        }
    }
}
