using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Dtos
{
    public class ListDto : RealmObject
    {
        [PrimaryKey]
        public string Key { get; set; }

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        
        public DateTimeOffset? ExpireAt { get; set; }

        public IList<string> Values { get; set; }
    }
}