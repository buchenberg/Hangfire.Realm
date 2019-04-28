using System;
using Realms;

namespace Hangfire.Realm.Models
{
    public class LockDto : RealmObject    
    {
        [PrimaryKey]
        public string Resource { get; set; }
        
        public DateTimeOffset? ExpireAt { get; set; }

        [Ignored] public bool IsAcquired => ExpireAt != null;
    }
}