using Realms;
using System;

namespace Hangfire.Realm.Models
{
    public class FieldDto : RealmObject, IKeyValue
    {
        public FieldDto() { }

        public FieldDto(string key, string value)
        {
            Key = key;
            Value = value;
        }

        public string Key { get; set; }
        public string Value { get; set; }
    }
}