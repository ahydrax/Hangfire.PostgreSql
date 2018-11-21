namespace Hangfire.PostgreSql.Kafka
{
    public static class KafkaJobUtils
    {
        public static string CreateTopicName(string queueName)
            => "hangfire-queue-" + queueName;
    }
}
