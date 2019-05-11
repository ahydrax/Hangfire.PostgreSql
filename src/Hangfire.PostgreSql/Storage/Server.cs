using System;
using Hangfire.Common;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Storage
{
    internal sealed partial class StorageConnection : JobStorageConnection
    {
        public override void AnnounceServer(string serverId, ServerContext context)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));
            Guard.ThrowIfNull(context, nameof(context));

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow
            };

            const string query = @"
INSERT INTO server (id, data, lastheartbeat)
VALUES (@id, @data, NOW() AT TIME ZONE 'UTC')
ON CONFLICT (id)
DO UPDATE SET data = @data, lastheartbeat = NOW() AT TIME ZONE 'UTC'
";
            var parameters = new { id = serverId, data = SerializationHelper.Serialize(data) };
            _connectionProvider.Execute(query, parameters);
        }

        public override void Heartbeat(string serverId)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));

            const string query = @"UPDATE server SET lastheartbeat = @heartbeat WHERE id = @id;";
            var parameters = new { id = serverId, heartbeat = DateTime.UtcNow };
            _connectionProvider.Execute(query, parameters);
        }

        public override void RemoveServer(string serverId)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));

            const string query = @"DELETE FROM server WHERE id = @id;";
            _connectionProvider.Execute(query, new { id = serverId });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            Guard.ThrowIfValueIsNotPositive(timeOut, nameof(timeOut));

            const string query = @"DELETE FROM server WHERE lastheartbeat < NOW() AT TIME ZONE 'UTC' - @timeout";
            return _connectionProvider.Execute(query, new { timeout = timeOut });
        }
    }
}
