using System;
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
    internal sealed class PollingJobQueueProvider : IJobQueueProvider, IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(PollingJobQueueProvider));

        private readonly IConnectionProvider _connectionProvider;
        private readonly TimeSpan _timeoutBetweenPasses;
        private string[] _queues = { "default" };

        public PollingJobQueueProvider(IConnectionProvider connectionProvider, TimeSpan timeoutBetweenPasses)
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

            Logger.Info("Updating queue list.");
            using (var connectionHolder = _connectionProvider.AcquireConnection())
            {
                var servers = connectionHolder.FetchList<string>(@"select data from server");
                var jobQueues = connectionHolder.FetchList<string>(@"select distinct queue from jobqueue;");

                _queues = servers
                    .Select(SerializationHelper.Deserialize<ServerData>)
                    .SelectMany(x => x.Queues)
                    .Concat(jobQueues)
                    .Distinct()
                    .OrderBy(x => x)
                    .ToArray();
            }

            Logger.Info($"Queue list updated. Found {_queues.Length} queues.");

            cancellationToken.Wait(_timeoutBetweenPasses);
        }
    }
}
