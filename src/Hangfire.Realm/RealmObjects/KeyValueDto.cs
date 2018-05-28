using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class KeyValueDto : RealmObject
    {
		public string Key { get; set; }
		public string Value { get; set; }
    }
}
