using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Entities;
using Hangfire.Server;

namespace Hangfire.PostgreSql.Queueing
{
#pragma warning disable 618 // TODO Remove when Hangfire 2.0 will be released
    internal sealed class MonitoringJobQueueProvider : IJobQueueProvider, IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MonitoringJobQueueProvider));

        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _timeoutBetweenPasses;
        private string[] _queues = { "default" };

        public MonitoringJobQueueProvider(IConnectionProvider connectionProvider, TimeSpan timeoutBetweenPasses)
        {
            Guard.ThrowIfNull(connectionProvider, nameof(connectionProvider));
            Guard.ThrowIfValueIsNotPositive(timeoutBetweenPasses, nameof(timeoutBetweenPasses));

            _connectionProvider = connectionProvider;
            _timeoutBetweenPasses = timeoutBetweenPasses;
        }

        public string[] GetQueues() => _queues;

        public void Execute(BackgroundProcessContext context) => Execute(context.StoppingToken);

        public void Execute(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            const string query = @"select * from server";
            var servers = _connectionProvider.FetchList<Entities.Server>(query);

            var queues = new HashSet<string>();
            foreach (var server in servers)
            {
                var serverData = SerializationHelper.Deserialize<ServerData>(server.Data);
                foreach (var queueName in serverData.Queues)
                {
                    queues.Add(queueName);
                }
            }
            _queues = queues.OrderBy(x => x).ToArray();

            Logger.Info("Queues info updated");

            cancellationToken.Wait(_timeoutBetweenPasses);
        }
    }
}
