using System;
using Dapper;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class NpgsqlTransactionConnectionProvider : IConnectionProvider
    {
        private readonly NpgsqlTransaction _transaction;
        private readonly string _hangfireSchemaName;
        private string _currentSchemaName;

        public NpgsqlTransactionConnectionProvider(NpgsqlTransaction transaction, string hangfireSchema)
        {
            _transaction = transaction;
            _hangfireSchemaName = hangfireSchema;
        }

        public ConnectionHolder AcquireConnection()
        {
            // we actually can't dispose the connection
            // since the user might still need it
            _currentSchemaName = _transaction.Connection.QueryFirst<string>("SHOW search_path");
            _transaction.Connection.Execute($@"SET search_path={_hangfireSchemaName}");
            var savepoint = "HANGFIRE" + Guid.NewGuid().ToString().Replace("-", "");
            _transaction.Save(savepoint);
            return new ConnectionHolder(_transaction.Connection, holder =>
            {
                _transaction.Release(savepoint);
                _transaction.Connection.Execute($@"SET search_path={_currentSchemaName ?? "public"}");
            }, _transaction);
        }

        public void Dispose()
        {
            // Npgsql will handle disposing of internal pool by itself
        }
    }
}