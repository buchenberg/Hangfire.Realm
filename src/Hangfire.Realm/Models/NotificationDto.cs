using Realms;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.Realm.Models
{
    public class NotificationDto : RealmObject
    {
        [PrimaryKey]
        public string Id { get; set; } = Guid.NewGuid().ToString();

        public string Type { get; set; }

        public string Value { get; set; }
    }
}
