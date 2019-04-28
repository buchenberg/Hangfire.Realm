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
	    
	    public RealmJobStorage(RealmJobStorageOptions options)
	    {
		    _options = options;
		    options.RealmConfiguration.SchemaVersion = SchemaVersion;
		    
			_realmDbContext = new RealmDbContext(options.RealmConfiguration);
	    }
	    public override IMonitoringApi GetMonitoringApi()
	    {
		    return new RealmMonitoringApi(_realmDbContext);
	    }

	    public override IStorageConnection GetConnection()
	    {
		    return new RealmStorageConnection(_realmDbContext, _options);
	    }
	    
	    
    }
}