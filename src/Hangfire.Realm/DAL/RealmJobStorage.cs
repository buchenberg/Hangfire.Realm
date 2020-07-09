using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Realm.DAL
{
    public class RealmJobStorage : JobStorage
    {
        private static readonly object LockObject = new object();
        internal TimeSpan? SlidingInvisibilityTimeout => Options.SlidingInvisibilityTimeout;
        internal TimeSpan? DistributedLockLifetime => Options.DistributedLockLifetime;

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
            return new RealmStorageConnection(this);
	    }


        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, Options.JobExpirationCheckInterval);
            // yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        public Realms.Realm GetRealm()
        {
            lock (RealmJobStorage.LockObject)
            {
                return Realms.Realm.GetInstance(Options.RealmConfiguration);
            }
        }


    }
}