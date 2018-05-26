using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class StateRealmObject : RealmObject
    {
	    public string Name { get; set; }

	    public string Reason { get; set; }

	    public DateTimeOffset CreatedAt { get; set; }

	    public IList<KeyValueRealmObject> Data { get; } = new List<KeyValueRealmObject>();
    }
}
