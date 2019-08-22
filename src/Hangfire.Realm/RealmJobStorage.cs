using System;
using Hangfire.Storage;
using Realms;
using Realms.Sync;

namespace Hangfire.Realm
{
    public class RealmJobStorage : JobStorage
    {
        internal TimeSpan? SlidingInvisibilityTimeout => Options.SlidingInvisibilityTimeout;

        public RealmJobStorage(RealmJobStorageOptions options)
	    {
		    Options = options ?? throw new ArgumentNullException(nameof(options));
            SchemaVersion = options.RealmConfiguration.SchemaVersion;
        }

        public ulong SchemaVersion { get; set; }
        public RealmJobStorageOptions Options { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
	    {
     	    return new RealmMonitoringApi(this);
	    }

	    public override IStorageConnection GetConnection()
	    {
		    return new RealmStorageConnection(this, JobQueueSemaphore.Instance);
	    }

        public Realms.Realm GetRealm()
        {
            return Realms.Realm.GetInstance(Options.RealmConfiguration);
        }

    }
}