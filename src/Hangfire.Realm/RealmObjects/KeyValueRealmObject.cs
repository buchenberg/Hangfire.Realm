using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class KeyValueRealmObject : RealmObject
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}