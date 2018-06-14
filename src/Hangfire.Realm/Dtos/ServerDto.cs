using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Dtos
{
    public class ServerDto : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;

        public DateTimeOffset? LastHeartbeat { get; set; }
        
        public int WorkerCount { get; set; }

        public IList<string> Queues { get; }

        public DateTimeOffset? StartedAt { get; set; }
    }
}