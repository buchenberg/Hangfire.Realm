using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Models
{
    public class SetDto : RealmObject, IExpireAt
    {
        public string Key { get; set; } 

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        
        public DateTimeOffset? ExpireAt { get; set; }

        public string Value { get; set; }
        
        public double Score { get; set; }

    }
}