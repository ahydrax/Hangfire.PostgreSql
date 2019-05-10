using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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

        public static T Fetch<T>(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            where T : struct
            => connectionHolder.Connection.QueryFirstOrDefault<T?>(sql, param, transaction, commandTimeout, commandType) ?? default(T);

        public static List<T> FetchList<T>(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            var result = connectionHolder.Connection.Query<T>(sql, param, transaction, true, commandTimeout, commandType);
            return result as List<T> ?? new List<T>(result);
        }

        public static T FetchFirstOrDefault<T>(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            => connectionHolder.Connection.QueryFirstOrDefault<T>(sql, param, transaction, commandTimeout, commandType);
    }
}
