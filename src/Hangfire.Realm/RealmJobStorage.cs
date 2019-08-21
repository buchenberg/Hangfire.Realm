using System;
using Hangfire.Storage;
using Realms;
using Realms.Sync;

namespace Hangfire.Realm
{
    public class RealmJobStorage : JobStorage
    {
        
	    private readonly IRealmDbContext _realmDbContext;
        internal TimeSpan? SlidingInvisibilityTimeout => Options.SlidingInvisibilityTimeout;

        public RealmJobStorage(RealmJobStorageOptions options)
	    {
		    Options = options;
            SchemaVersion = options.RealmConfiguration.SchemaVersion;
			_realmDbContext = new RealmDbContext(options.RealmConfiguration);
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

        public IRealmDbContext GetDbContext()
        {
           return _realmDbContext;
        }

    }
}