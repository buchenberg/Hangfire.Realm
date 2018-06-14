using System;
using Realms;

namespace Hangfire.Realm.Dtos
{
    public class CounterDto : RealmObject
    {
        [PrimaryKey]
        public string Key { get; set; }
        
        public RealmInteger<long> Value { get; set; }
        
        public DateTimeOffset? ExpireIn { get; set; }
    }
}