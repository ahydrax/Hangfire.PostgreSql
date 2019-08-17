using System.Collections.Generic;
using System.Data;
using Dapper;
using Hangfire.Annotations;

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
                return connectionHolder.Connection.Execute(
                    sql,
                    param,
                    transaction,
                    commandTimeout,
                    commandType);
            }
        }

        [CanBeNull]
        public static T Fetch<T>(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                return connectionHolder.Fetch<T>(
                    sql,
                    param,
                    transaction,
                    commandTimeout,
                    commandType);
            }
        }

        public static T FetchScalar<T>(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
            where T : struct
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                return connectionHolder.FetchScalar<T>(
                    sql,
                    param,
                    transaction,
                    commandTimeout,
                    commandType);
            }
        }

        [NotNull]
        public static List<T> FetchList<T>(this IConnectionProvider connectionProvider,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (var connectionHolder = connectionProvider.AcquireConnection())
            {
                return connectionHolder.FetchList<T>(
                    sql,
                    param,
                    transaction,
                    commandTimeout,
                    commandType);
            }
        }
    }
}
