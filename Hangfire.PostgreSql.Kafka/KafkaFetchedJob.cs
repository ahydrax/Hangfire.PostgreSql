using Confluent.Kafka;
using Hangfire.Storage;

namespace Hangfire.PostgreSql.Kafka
{
    public class KafkaFetchedJob : IFetchedJob
    {
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;
        private readonly string _queue;
        private readonly Producer<string, string> _producer;

        public KafkaFetchedJob(string queue, string jobId, Producer<string, string> producer)
        {
            JobId = jobId;
            _queue = queue;
            _producer = producer;
        }

        public string JobId { get; }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }

            _disposed = true;
        }

        public void RemoveFromQueue()
        {
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            _producer.ProduceAsync(KafkaJobUtils.CreateTopicName(_queue),
                new Message<string, string> { Key = _queue, Value = JobId }).GetAwaiter().GetResult();
            _requeued = true;
        }
    }
}
