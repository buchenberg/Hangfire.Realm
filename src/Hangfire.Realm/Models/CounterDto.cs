using System;
using Realms;

namespace Hangfire.Realm.Models
{
    public class CounterDto : RealmObject
    {
        public CounterDto()
        {
        }

        public CounterDto(string key)
        {
            Key = key;
        }

        [PrimaryKey]
        public string Key { get; set; }

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;

        public RealmInteger<long> Value { get; set; } = 0;
        
        public DateTimeOffset? ExpireAt { get; set; }
    }
}