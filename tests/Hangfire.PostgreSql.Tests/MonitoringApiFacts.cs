using Hangfire.PostgreSql.Connectivity;
using Hangfire.PostgreSql.Tests.Utils;
using Xunit;

namespace Hangfire.PostgreSql.Tests
{
    public class MonitoringApiFacts
    {
        private readonly IConnectionProvider _connectionProvider;
        private readonly MonitoringApi _monitoringApi;

        public MonitoringApiFacts()
        {
            _connectionProvider = ConnectionUtils.GetConnectionProvider();
            _monitoringApi = new MonitoringApi(_connectionProvider);
        }

        [Fact, CleanDatabase]
        public void ScheduledCount_ReturnsActualValue()
        {
            
        }
    }
}
