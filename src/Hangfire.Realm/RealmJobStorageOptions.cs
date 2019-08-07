using System;
using Realms;

namespace Hangfire.Realm
{
    public class RealmJobStorageOptions
    {
        private TimeSpan _queuePollInterval;
        public TimeSpan DistributedLockLifetime { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Invisibility timeout
        /// </summary>
        [Obsolete("This is marked obsolete in Hangfire.")]
        public TimeSpan InvisibilityTimeout { get; set; }
        public TimeSpan QueuePollInterval
        {
            get { return _queuePollInterval; }
            set
            {
                var message = $"The QueuePollInterval property value should be positive. Given: {value}.";

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, nameof(value));
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, nameof(value));
                }

                _queuePollInterval = value;
            }
        }


        public RealmConfigurationBase RealmConfiguration { get; set; }
    }
}
