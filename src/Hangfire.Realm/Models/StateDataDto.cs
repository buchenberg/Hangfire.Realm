using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Models
{
    public class StateDataDto : RealmObject, IKeyValue
    {
        public string Key { get; set; }
        public string Value { get; set; }
		
        public StateDataDto()
        {
		    
        }

        public StateDataDto(KeyValuePair<string, string> pair) : this(pair.Key, pair.Value)
        {
        }
        
        public StateDataDto(string key, string value)
        {
            Key = key;
            Value = value;
        }
    }
}