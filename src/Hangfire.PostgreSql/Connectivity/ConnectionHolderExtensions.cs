using System.Collections.Generic;
using System.Data;
using Dapper;
using Hangfire.Annotations;

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
            => connectionHolder.Connection.Execute(
                sql,
                param,
                transaction,
                commandTimeout,
                commandType);

        [CanBeNull]
        public static T Fetch<T>(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            => connectionHolder.Connection.QueryFirstOrDefault<T>(
                   sql,
                   param,
                   transaction,
                   commandTimeout,
                   commandType);

        public static T FetchScalar<T>(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            where T : struct
            => connectionHolder.Connection.QueryFirstOrDefault<T?>(
                   sql,
                   param,
                   transaction,
                   commandTimeout,
                   commandType)
               ?? default;

        [NotNull]
        public static List<T> FetchList<T>(this ConnectionHolder connectionHolder,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            var result = connectionHolder.Connection.Query<T>(
                sql,
                param,
                transaction,
                true,
                commandTimeout,
                commandType);
            return result as List<T> ?? new List<T>(result);
        }
    }
}
