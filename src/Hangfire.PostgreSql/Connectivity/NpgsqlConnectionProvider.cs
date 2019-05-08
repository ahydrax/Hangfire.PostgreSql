using System.Data;

namespace Hangfire.PostgreSql.Connectivity
{
    internal sealed class NpgsqlConnectionProvider : IConnectionProvider
    {
        private readonly IConnectionBuilder _connectionBuilder;

        public NpgsqlConnectionProvider(IConnectionBuilder connectionBuilder)
        {
            _connectionBuilder = connectionBuilder;
        }

        public ConnectionHolder AcquireConnection()
        {
            var connection = _connectionBuilder.Build();
            connection.Open();
            return new ConnectionHolder(connection, holder => holder.Connection.Dispose());
        }

        public void Dispose()
        {
            // Npgsql will handle disposing of internal pool by itself
        }
    }
}
