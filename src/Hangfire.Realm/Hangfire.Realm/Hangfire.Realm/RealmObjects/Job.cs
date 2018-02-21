using System;
using System.Collections.Generic;
using System.Text;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class Job : RealmObject
    {
		[PrimaryKey]
	    public string Id { get; set; }

	    public string StateName { get; set; }

	    public string InvocationData { get; set; }

	    public string Arguments { get; set; }

	    public Dictionary<string, string> Parameters { get; set; } = new Dictionary<string, string>();

	    public State[] StateHistory { get; set; } = new State[0];

	    public DateTime CreatedAt { get; set; }

	    public DateTime? ExpireAt { get; set; }
    }
}
