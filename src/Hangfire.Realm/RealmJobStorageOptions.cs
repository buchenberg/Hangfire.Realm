using System;
using Realms;

namespace Hangfire.Realm
{
    public class RealmJobStorageOptions
    {
        private TimeSpan _queuePollInterval;
        private TimeSpan _jobExpirationCheckInterval;
        private TimeSpan? _slidingInvisibilityTimeout;

        public RealmJobStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromSeconds(15);
            SlidingInvisibilityTimeout = null;
            JobExpirationCheckInterval = TimeSpan.FromMinutes(30);
        }
        public TimeSpan DistributedLockLifetime { get; set; } = TimeSpan.FromSeconds(30);
        public RealmConfigurationBase RealmConfiguration { get; set; }
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
        public TimeSpan? SlidingInvisibilityTimeout
        {
            get { return _slidingInvisibilityTimeout; }
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException("Sliding timeout should be greater than zero");
                }

                _slidingInvisibilityTimeout = value;
            }
        }
        public TimeSpan JobExpirationCheckInterval
        {
            get { return _jobExpirationCheckInterval; }
            set
            {
                if (value.TotalMilliseconds > int.MaxValue)
                {
                    throw new ArgumentOutOfRangeException("Job expiration check interval cannot be greater than int.MaxValue");
                }
                _jobExpirationCheckInterval = value;
            }
        }

    }
}
