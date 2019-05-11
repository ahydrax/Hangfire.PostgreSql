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
insert into server (id, data, lastheartbeat)
values (@id, @data, now() at time zone 'UTC')
on conflict (id)
do update set data = @data, lastheartbeat = now() at time zone 'UTC'
";
            var parameters = new { id = serverId, data = SerializationHelper.Serialize(data) };
            _connectionProvider.Execute(query, parameters);
        }

        public override void Heartbeat(string serverId)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));

            const string query = @"update server set lastheartbeat = now() at time zone 'UTC' where id = @id;";
            var parameters = new { id = serverId };
            _connectionProvider.Execute(query, parameters);
        }

        public override void RemoveServer(string serverId)
        {
            Guard.ThrowIfNull(serverId, nameof(serverId));

            const string query = @"delete from server where id = @id;";
            _connectionProvider.Execute(query, new { id = serverId });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            Guard.ThrowIfValueIsNotPositive(timeOut, nameof(timeOut));

            const string query = @"delete from server where lastheartbeat < now() at time zone 'UTC' - @timeout";
            return _connectionProvider.Execute(query, new { timeout = timeOut });
        }
    }
}
