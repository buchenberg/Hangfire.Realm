using System;
using Realms;

namespace Hangfire.Realm.Models
{
    public class CounterDto : RealmObject
    {
        [PrimaryKey]
        public string Key { get; set; }

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;

        public RealmInteger<long> Value { get; set; }
        
        public DateTimeOffset? ExpireAt { get; set; }
    }
}