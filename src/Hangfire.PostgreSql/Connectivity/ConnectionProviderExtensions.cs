using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    internal static class ConnectionProviderExtensions
    {
        public static int Execute(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.Execute(sql, param, transaction, commandTimeout, commandType);
            }
        }

        public static T Fetch<T>(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            where T : struct
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                return connectionHolder.Fetch<T>(sql, param, transaction, commandTimeout, commandType);
            }
        }

        public static List<T> FetchList<T>(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                var result = connectionHolder.Connection.Query<T>(sql, param, transaction, true, commandTimeout, commandType);
                return result as List<T> ?? new List<T>(result);
            }
        }

        public static T FetchFirstOrDefault<T>(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                return connectionHolder.Connection.QueryFirstOrDefault<T>(sql, param, transaction, commandTimeout,
                    commandType);
            }
        }
    }
}
