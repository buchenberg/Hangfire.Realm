using System;
using Realms;

namespace Hangfire.Realm.Dtos
{
    public class SetDto : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;
        public string Key { get; set; }
        public string Value { get; set; }
        public DateTimeOffset? ExpireIn { get; set; }
        public double Score { get; set; }
    }
}