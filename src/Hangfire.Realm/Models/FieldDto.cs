using Realms;
using System;

namespace Hangfire.Realm.Models
{
    public class FieldDto : RealmObject, IKeyValue
    {
        [PrimaryKey]
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string Key { get; set; }
        public string Value { get; set; }
		
    }
}