using Realms;

namespace Hangfire.Realm.Models
{
    public class ParameterDto : RealmObject, IKeyValue
    {
        public string Key { get; set; }
        public string Value { get; set; }
		
        public ParameterDto()
        {
		    
        }

        public ParameterDto(string key, string value)
        {
            Key = key;
            Value = value;
        }
    }
}