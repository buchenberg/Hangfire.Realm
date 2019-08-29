using System;
using Realms;

namespace Hangfire.Realm.Models
{
	public class JobQueueDto : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; } = Guid.NewGuid().ToString();


        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;

	    public string JobId { get; set; }

	    public string Queue { get; set; }

	    public DateTimeOffset? FetchedAt { get; set; }
    }
}
