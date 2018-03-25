using System;

namespace Hangfire.Realm.RealmObjects
{
    public class ExpiringGenericRealmObject : GenericRealmObject
    {
        public DateTimeOffset? ExpireAt { get; set; }
    }
}