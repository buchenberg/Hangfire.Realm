using System;
using Hangfire.Storage;
using Realms;
using Realms.Sync;

namespace Hangfire.Realm
{
    public class RealmJobStorage : JobStorage
    {
	    private readonly RealmJobStorageOptions _options;
	    public const int SchemaVersion = 1;
	    private readonly IRealmDbContext _realmDbContext;
        internal TimeSpan? SlidingInvisibilityTimeout => _options.SlidingInvisibilityTimeout;

        public RealmJobStorage(RealmJobStorageOptions options)
	    {
		    _options = options;
		    options.RealmConfiguration.SchemaVersion = SchemaVersion;
			_realmDbContext = new RealmDbContext(options.RealmConfiguration);
            InitializeQueueProviders();
        }
	    public override IMonitoringApi GetMonitoringApi()
	    {
     	    return new RealmMonitoringApi(this);
	    }

	    public override IStorageConnection GetConnection()
	    {
		    return new RealmStorageConnection(_realmDbContext, _options);
	    }

        public IRealmDbContext GetDbContext()
        {
           return _realmDbContext;
        }
        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new RealmJobQueueProvider(this, _options);
            QueueProviders = new RealmJobQueueProviderCollection(defaultQueueProvider);
        }

        public virtual RealmJobQueueProviderCollection QueueProviders { get; private set; }

    }
}