using System;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    public class RealmJobStorage : JobStorage
    {
	    public override IMonitoringApi GetMonitoringApi()
	    {
		    return new RealmMonitoringApi();
	    }

	    public override IStorageConnection GetConnection()
	    {
		    return new RealmStorageConnection();
	    }
    }
}