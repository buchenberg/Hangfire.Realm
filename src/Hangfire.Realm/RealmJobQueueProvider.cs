using Hangfire.Annotations;
using Hangfire.Storage;
using System;

namespace Hangfire.Realm
{
    public class RealmJobQueueProvider
    {
        private readonly RealmJobQueue _jobQueue;
        private readonly IMonitoringApi _monitoringApi;

        public RealmJobQueueProvider(RealmJobStorage storage, RealmJobStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            _jobQueue = new RealmJobQueue(storage, options);
            _monitoringApi = storage.GetMonitoringApi();
        }


        public RealmJobQueue GetJobQueue()
        {
            return _jobQueue;
        }

        public IMonitoringApi GetJobQueueMonitoringApi()
        {
            return _monitoringApi;
        }
    }
}