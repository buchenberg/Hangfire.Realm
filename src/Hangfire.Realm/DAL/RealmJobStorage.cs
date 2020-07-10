using Hangfire.Server;
using Hangfire.Storage;
using System;
using System.Collections.Generic;

namespace Hangfire.Realm.DAL
{
    public class RealmJobStorage : JobStorage
    {
        private readonly object _lockObject;
        internal TimeSpan? SlidingInvisibilityTimeout => Options.SlidingInvisibilityTimeout;
        internal TimeSpan? DistributedLockLifetime => Options.DistributedLockLifetime;

        public RealmJobStorage(RealmJobStorageOptions options)
	    {
		    Options = options ?? throw new ArgumentNullException(nameof(options));
            SchemaVersion = options.RealmConfiguration.SchemaVersion;
            _lockObject = new object();
        }

        public ulong SchemaVersion { get; set; }
        public RealmJobStorageOptions Options { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
	    {
     	    return new RealmMonitoringApi(this);
	    }

	    public override IStorageConnection GetConnection()
	    {
            return new RealmStorageConnection(this);
	    }


        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, Options.JobExpirationCheckInterval);
            // yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        public Realms.Realm GetRealm()
        {
            lock (_lockObject)
            {
                return Realms.Realm.GetInstance(Options.RealmConfiguration);
            }
        }


    }
}