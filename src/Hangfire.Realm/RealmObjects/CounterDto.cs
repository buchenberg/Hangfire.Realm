using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class CounterDto : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset Created { get; set; }
        public string Key { get; set; }
        public DateTimeOffset? ExpireAt { get; set; }
        public long Value { get; set; }
    }
}