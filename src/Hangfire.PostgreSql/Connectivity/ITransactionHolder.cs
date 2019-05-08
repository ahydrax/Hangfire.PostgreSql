using System;
using Npgsql;

namespace Hangfire.PostgreSql.Connectivity
{
    public interface ITransactionHolder : IDisposable
    {
        NpgsqlTransaction Transaction { get; }
        void Commit();
    }
}