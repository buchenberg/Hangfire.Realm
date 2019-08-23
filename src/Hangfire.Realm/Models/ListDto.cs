using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Models
{
    public class ListDto : RealmObject, IExpireAt
    {
        public ListDto()
        {
        }

        public ListDto(string key)
        {
            Key = key;
        }
        public string Key { get; set; }

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        
        public DateTimeOffset? ExpireAt { get; set; }

        public IList<string> Values { get; }
    }
}