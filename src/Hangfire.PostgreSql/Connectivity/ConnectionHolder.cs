using System;
using System.Data;
using System.Data.Common;
using System.Transactions;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class ConnectionHolder : IDisposable
    {
        private readonly NpgsqlTransaction _transaction;
        private readonly NpgsqlConnection _connection;
        private readonly Action<ConnectionHolder> _connectionDisposer;

        public ConnectionHolder(NpgsqlConnection connection, Action<ConnectionHolder> connectionDisposer,
            NpgsqlTransaction transaction = null)
        {
            _connection = connection;
            _connectionDisposer = connectionDisposer;
            _transaction = transaction;
        }

        public NpgsqlConnection Connection
        {
            get
            {
                if (Disposed)
                {
                    throw new ObjectDisposedException(nameof(Connection));
                }

                if (_connection.State == ConnectionState.Closed)
                {
                    Disposed = true;
                    throw new ObjectDisposedException(nameof(Connection));
                }

                return _connection;
            }
        }

        public TransactionHolder BeginTransaction(System.Data.IsolationLevel level)
        {
            if (_transaction != null)
            {
                return new TransactionHolder(_transaction, false, holder => { });
            }

            if (Transaction.Current != null)
            {
                return new TransactionHolder(null, false, holder => { });
            }

            var transaction = _connection.BeginTransaction(level);
            return new TransactionHolder(transaction, true, holder => holder.Transaction.Dispose());
        }

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            if (Disposed) return;

            _connectionDisposer(this);
            Disposed = true;
        }
    }
}