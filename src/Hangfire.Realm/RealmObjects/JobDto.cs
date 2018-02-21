using System;
using System.Collections.Generic;
using System.Text;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class JobDto : RealmObject
    {
		[PrimaryKey]
	    public string Id { get; set; }

	    public string StateName { get; set; }

	    public string InvocationData { get; set; }

	    public string Arguments { get; set; }

	    public KeyValueDto[] Parameters { get; set; } = new KeyValueDto[0];

	    public StateDto[] StateHistory { get; set; } = new StateDto[0];

	    public DateTimeOffset CreatedAt { get; set; }

	    public DateTimeOffset? ExpireAt { get; set; }
    }
}
