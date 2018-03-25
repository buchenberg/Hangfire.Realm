using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class JobQueueRealmObject : RealmObject
    {
		[PrimaryKey]
	    public string Id { get; set; }

	    public string JobId { get; set; }

	    public string Queue { get; set; }

	    public DateTimeOffset? FetchedAt { get; set; }
    }
}
