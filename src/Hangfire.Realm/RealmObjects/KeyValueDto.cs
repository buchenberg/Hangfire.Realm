using Realms;

namespace Hangfire.Realm.RealmObjects
{
    internal class KeyValueDto : RealmObject
    {
        public string Key { get; set; }
        public string Value { get; set; }
    }
}