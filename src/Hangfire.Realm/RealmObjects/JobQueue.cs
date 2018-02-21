using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class JobQueue : RealmObject
    {
		[PrimaryKey]
	    public string Id { get; set; }

	    public string JobId { get; set; }

	    public string Queue { get; set; }

	    public DateTime? FetchedAt { get; set; }
    }
}
