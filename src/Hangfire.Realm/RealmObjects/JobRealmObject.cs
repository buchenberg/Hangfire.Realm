using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class JobRealmObject : RealmObject
    {
		[PrimaryKey]
	    public string Id { get; set; }

	    public string StateName { get; set; }

	    public string InvocationData { get; set; }

	    public string Arguments { get; set; }

	    public IList<KeyValueRealmObject> Parameters { get; } = new List<KeyValueRealmObject>();

	    public IList<StateRealmObject> StateHistory { get; } = new List<StateRealmObject>();

		public DateTimeOffset CreatedAt { get; set; }

	    public DateTimeOffset? ExpireAt { get; set; }
    }
}
