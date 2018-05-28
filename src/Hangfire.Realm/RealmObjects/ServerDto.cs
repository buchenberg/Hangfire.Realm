using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class ServerDto : RealmObject, IEntity
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset Created { get; set; }

        public DateTimeOffset? LastHeartbeat { get; set; }
        
        public int WorkerCount { get; set; }

        public IList<string> Queues { get; }

        public DateTimeOffset? StartedAt { get; set; }
    }
}