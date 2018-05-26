using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class ServerRealmObject : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset? LastHeartbeat { get; set; }
        
        public int WorkerCount { get; set; }

        public IList<string> Queues { get; } = new List<string>();

        public DateTimeOffset? StartedAt { get; set; }
    }
}