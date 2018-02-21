using System;
using System.Collections.Generic;

namespace Hangfire.Realm.RealmObjects
{
	internal class State
    {
	    public string Name { get; set; }

	    public string Reason { get; set; }

	    public DateTime CreatedAt { get; set; }

	    public Dictionary<string, string> Data { get; set; }
    }
}
