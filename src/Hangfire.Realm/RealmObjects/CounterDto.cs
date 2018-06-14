using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class CounterDto : RealmObject
    {
        [PrimaryKey]
        public string Key { get; set; }
        
        public RealmInteger<long> Value { get; set; }
    }
}