using System;
using System.Data;
using Dapper;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal static class ConnectionHolderExtensions
    {
        public static int Execute(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            => connectionHolder.Connection.Execute(sql, param, transaction, commandTimeout, commandType);
    }
}
