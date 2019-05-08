using System;
using System.Data;

namespace Hangfire.PostgreSql.Connectivity
{
    internal interface IConnectionProvider : IDisposable
    {
        ConnectionHolder AcquireConnection();
    }
}
