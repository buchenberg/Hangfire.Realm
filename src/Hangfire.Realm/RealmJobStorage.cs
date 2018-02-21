using System;
using Hangfire.Storage;
using Realms;

namespace Hangfire.Realm
{
    public class RealmJobStorage : JobStorage
    {
	    public const int SchemaVersion = 1;

	    private readonly RealmJobStorageOptions _options;
		
	    public RealmJobStorage(RealmJobStorageOptions options)
	    {
		    _options = options;
			_options.RealmConfiguration= new RealmConfiguration(options.DatabasePath)
			{
				SchemaVersion = SchemaVersion
				
			};
	    }
	    public override IMonitoringApi GetMonitoringApi()
	    {
		    return new RealmMonitoringApi(_options);
	    }

	    public override IStorageConnection GetConnection()
	    {
		    return new RealmStorageConnection();
	    }
    }
}