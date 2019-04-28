using System;
using Realms;

namespace Hangfire.Realm
{
    public class RealmJobStorageOptions
    {
	    public TimeSpan DistributedLockLifetime { get; set; } = TimeSpan.FromSeconds(30);
		    
		    
		public RealmConfigurationBase RealmConfiguration { get; set; }
    }
}
