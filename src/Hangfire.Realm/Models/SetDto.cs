using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Models
{
    public class SetDto : RealmObject, IExpireAt
    {
        public SetDto()
        {
        }

        public SetDto(string key, string value, double score)
        {
            Key = key;
            Value = value;
            Score = score;
        }


        public string Key { get; set; } 

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        
        public DateTimeOffset? ExpireAt { get; set; }

        public string Value { get; set; }
        
        public double Score { get; set; }

    }
}