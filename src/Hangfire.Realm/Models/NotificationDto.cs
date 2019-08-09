using Realms;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.Realm.Models
{
    public enum NotificationType 
    {
        JobEnqueued = 0,
        LockReleased = 1
    }
    public class NotificationDto : RealmObject
    {
        private String enumDescription;
        public static NotificationDto JobEnqueued(string queue)
        {
            return new NotificationDto
            {
                Id = Guid.NewGuid().ToString(),
                Type = NotificationType.JobEnqueued.ToString(),
                Value = queue
            };
        }

        public static NotificationDto LockReleased(string resource)
        {
            return new NotificationDto
            {
                Id = Guid.NewGuid().ToString(),
                Type = NotificationType.LockReleased.ToString(),
                Value = resource
            };
        }

        [PrimaryKey]
        public string Id { get; set; }

        public string Type { get; set; }

        public string Value { get; set; }
    }
}
