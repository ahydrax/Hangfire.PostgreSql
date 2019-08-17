namespace Hangfire.PostgreSql.Queueing
{
    internal interface IJobQueueProvider
    {
        string[] GetQueues();
    }
}
