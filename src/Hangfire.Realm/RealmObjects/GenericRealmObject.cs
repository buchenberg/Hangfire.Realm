using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class GenericRealmObject : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }
        public string Key { get; set; }
        public object Value { get; set; }
    }
}