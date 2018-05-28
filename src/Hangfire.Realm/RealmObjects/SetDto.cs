using System;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
    public class SetDto : RealmObject, IEntity
    {
        [PrimaryKey]
        public string Id { get; set; }

        public DateTimeOffset Created { get; set; }
        public string Key { get; set; }
        public double Score { get; set; }
    }
}