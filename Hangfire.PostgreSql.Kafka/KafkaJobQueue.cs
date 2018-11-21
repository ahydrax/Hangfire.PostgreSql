using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.PostgreSql.Kafka
{
    public class KafkaJobQueue : IExtendedJobQueue
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;
        private readonly AdminClientConfig _adminConfig;

        private readonly Lazy<Producer<string, string>> _lazyProducer;
        private readonly Lazy<Consumer<string, string>> _lazyConsumer;
        private readonly Lazy<AdminClient> _lazyAdminClient;

        public KafkaJobQueue()
        {
            _producerConfig = new ProducerConfig
            {
                GroupId = "hangfire",
                BootstrapServers = "localhost:9092",
            };
            _lazyProducer = new Lazy<Producer<string, string>>(() => new Producer<string, string>(_producerConfig));

            _consumerConfig = new ConsumerConfig
            {
                GroupId = "hangfire",
                BootstrapServers = "localhost:9092"
            };
            _lazyConsumer = new Lazy<Consumer<string, string>>(() =>
            {
                var consumer = new Consumer<string, string>(_consumerConfig);
                consumer.Subscribe(new[] { "default", "queue1", "queue2" }.Select(KafkaJobUtils.CreateTopicName));
                return consumer;
            });

            _adminConfig = new AdminClientConfig
            {
                GroupId = "hangfire",
                BootstrapServers = "localhost:9092"
            };
            _lazyAdminClient = new Lazy<AdminClient>(() => new AdminClient(_adminConfig));
        }

        public void Enqueue(string queue, string jobId)
        {
            var producer = _lazyProducer.Value;
            var topicName = KafkaJobUtils.CreateTopicName(queue);
            producer.ProduceAsync(topicName, new Message<string, string> { Key = queue, Value = jobId }).GetAwaiter().GetResult();
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            var consumer = _lazyConsumer.Value;

            var message = consumer.Consume(cancellationToken);

            return new KafkaFetchedJob(message.Key, message.Value, _lazyProducer.Value);
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var client = _lazyAdminClient.Value;


            return null;
        }

        public long EnqueuedCount(string queue)
        {
            var client = _lazyAdminClient.Value;

            var off = client.QueryWatermarkOffsets(
                new TopicPartition(KafkaJobUtils.CreateTopicName(queue), Partition.Any), TimeSpan.FromSeconds(1));

            return off.High.Value - off.Low.Value;
        }

        public long FetchedCount(string queue)
        {
            return 0;
        }

        public IEnumerable<string> GetQueues()
        {
            yield break;
        }
    }
}
