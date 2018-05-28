using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class JobQueueDto : RealmObject, IEntity
    {
		[PrimaryKey]
	    public string Id { get; set; }

	    public DateTimeOffset Created { get; set; }

	    public string JobId { get; set; }

	    public string Queue { get; set; }

	    public DateTimeOffset? FetchedAt { get; set; }
    }
}
