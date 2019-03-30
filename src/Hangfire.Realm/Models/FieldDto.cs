using Realms;

namespace Hangfire.Realm.Models
{
    public class FieldDto : RealmObject, IKeyValue
    {
        public string Key { get; set; }
        public string Value { get; set; }
		
        public FieldDto()
        {
		    
        }
    }
}