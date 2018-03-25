using System;
using System.Collections.Generic;
using System.Text;
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

	    public KeyValueRealmObject[] Parameters { get; set; } = new KeyValueRealmObject[0];

	    public StateRealmObject[] StateHistory { get; set; } = new StateRealmObject[0];

	    public DateTimeOffset CreatedAt { get; set; }

	    public DateTimeOffset? ExpireAt { get; set; }
    }
}
