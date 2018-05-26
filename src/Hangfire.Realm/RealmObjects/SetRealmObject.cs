using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class SetRealmObject : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }
        public string Key { get; set; }
        public double Score { get; set; }
    }
}