using System;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Hangfire.Logging;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class DefaultConnectionProvider : IConnectionProvider
    {
        public static readonly TimeSpan DisposeTimeout = TimeSpan.FromSeconds(10);

        private static readonly ILog Logger = LogProvider.GetLogger(typeof(DefaultConnectionProvider));

        private readonly IConnectionBuilder _connectionBuilder;
        private readonly ConcurrentBag<NpgsqlConnection> _connectionPool;
        private readonly Action<ConnectionHolder> _connectionDisposer;

        private volatile int _activeConnections;
        private readonly int _maxConnections;

        public DefaultConnectionProvider(IConnectionBuilder connectionBuilder)
        {
            Guard.ThrowIfNull(connectionBuilder, nameof(connectionBuilder));
            Guard.ThrowIfConnectionStringIsInvalid(connectionBuilder.ConnectionStringBuilder.ConnectionString);

            _connectionBuilder = connectionBuilder;
            _connectionPool = new ConcurrentBag<NpgsqlConnection>();
            _connectionDisposer = ReleaseConnection;

            _maxConnections = _connectionBuilder.ConnectionStringBuilder.MaxPoolSize;
        }

        internal int ActiveConnections => _activeConnections;

        public ConnectionHolder AcquireConnection()
        {
            var connection = GetFreeConnection();
            return new ConnectionHolder(connection, _connectionDisposer);
        }

        private void ReleaseConnection(ConnectionHolder connectionHolder)
        {
            if (connectionHolder.Disposed) return;
            _connectionPool.Add(connectionHolder.Connection);
        }

        private NpgsqlConnection GetFreeConnection()
        {
            var spinWait = new SpinWait();
            NpgsqlConnection connection;
            while (!_connectionPool.TryTake(out connection))
            {
                connection = CreateConnectionIfNeeded();
                if (connection != null) return connection;

                spinWait.SpinOnce();
            }
            return connection;
        }

        private NpgsqlConnection CreateConnectionIfNeeded()
        {
            if (_activeConnections >= _maxConnections) return null;
            Interlocked.Increment(ref _activeConnections);

            if (_activeConnections > _maxConnections)
            {
                Interlocked.Decrement(ref _activeConnections);
                return null;
            }

            NpgsqlConnection newConnection;
            try
            {
                newConnection = _connectionBuilder.Build();
                newConnection.Open();
            }
            catch (Exception e)
            {
                Logger.ErrorFormat("Error while creating connection", e);
                throw;
            }
            newConnection.StateChange += ConnectionStateChanged;

            return newConnection;
        }

        private void ConnectionStateChanged(object sender, StateChangeEventArgs stateChangeEventArgs)
        {
            if (stateChangeEventArgs.CurrentState != ConnectionState.Open)
            {
                Logger.Info("Connection was removed from pool");
                Interlocked.Decrement(ref _activeConnections);
            }
        }

        public void Dispose()
        {
            /*
             * Try to shutdown gracefully:
             * Wait for each connection become free.
             */
            var stopwatch = Stopwatch.StartNew();
            while (_activeConnections > 0)
            {
                if (_connectionPool.TryTake(out var connection))
                {
                    Interlocked.Decrement(ref _activeConnections);
                    connection.Dispose();
                }

                if (stopwatch.Elapsed > DisposeTimeout && _activeConnections > 0)
                {
                    var message = $"Disposing of connection pool took too long. Connections left: {_activeConnections}";
                    Logger.Error(message);
                    throw new TimeoutException(message);
                }
            }
        }
    }
}
