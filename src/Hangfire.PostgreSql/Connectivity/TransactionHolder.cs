using System;
using Dapper;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    public class TransactionHolder : ITransactionHolder
    {
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _transaction;
        private readonly Action<TransactionHolder> _transactionDisposer;
        private readonly bool _commitable;

        public TransactionHolder(NpgsqlTransaction transaction,
            bool commitable,
            Action<TransactionHolder> transactionDisposer)
        {
            _transaction = transaction;
            _transactionDisposer = transactionDisposer;
            _connection = connection;
            _commitable = commitable;
        }

        public NpgsqlTransaction Transaction
        {
            get
            {
                if (Disposed)
                {
                    throw new ObjectDisposedException(nameof(TransactionHolder));
                }

                return _transaction;
            }
        }

        public void Commit()
        {
            if (_commitable)
            {
                _transaction.Commit();
            }
        }

        public bool Disposed { get; private set; }

        public void Dispose()
        {
            if (Disposed) return;

            _transactionDisposer(this);
            Disposed = true;
        }
    }
}