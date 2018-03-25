using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class ServerRealmObject : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset? LastHeartbeat { get; set; }
        
        public int WorkerCount { get; set; }

        public string[] Queues { get; set; }

        public DateTimeOffset? StartedAt { get; set; }
    }
}