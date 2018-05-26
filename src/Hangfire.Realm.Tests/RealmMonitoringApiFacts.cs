using System;
using Hangfire.Realm.Tests.Utils;
using Xunit;

namespace Hangfire.Realm.Tests
{
    [Collection("Database")]
    public class RealmMonitoringApiFacts
    {
        private readonly RealmMonitoringApi _monitoringApi;
        
        public RealmMonitoringApiFacts()
        {
            var realm = ConnectionUtils.GetRealm();
            _monitoringApi = new RealmMonitoringApi(realm);
        }
        
        [Fact, CleanDatabase]
        public void GetStatistics_NoJobsExist_ReturnsZero()
        {
            // ARRANGE
            
            // ACT
            var result = _monitoringApi.GetStatistics();

            // ASSERT
            Assert.Equal(0, result.Enqueued);
            Assert.Equal(0, result.Failed);
            Assert.Equal(0, result.Processing);
            Assert.Equal(0, result.Scheduled);
        }
    }
}