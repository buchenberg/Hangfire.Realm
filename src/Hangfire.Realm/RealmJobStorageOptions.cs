﻿using System;
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
            SlidingInvisibilityTimeout = TimeSpan.FromMinutes(10);
            JobExpirationCheckInterval = TimeSpan.FromMinutes(30);
        }
        public TimeSpan DistributedLockLifetime { get; set; } = TimeSpan.FromSeconds(30);
        public RealmConfigurationBase RealmConfiguration { get; set; }
        public TimeSpan QueuePollInterval
        {
            get => _queuePollInterval;
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
            get => _slidingInvisibilityTimeout;
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(nameof(SlidingInvisibilityTimeout),"Sliding timeout should be greater than zero");
                }

                _slidingInvisibilityTimeout = value;
            }
        }
        public TimeSpan JobExpirationCheckInterval
        {
            get => _jobExpirationCheckInterval;
            set
            {
                if (value.TotalMilliseconds > int.MaxValue)
                {
                    throw new ArgumentOutOfRangeException(nameof(JobExpirationCheckInterval), "Job expiration check interval cannot be greater than int.MaxValue");
                }
                _jobExpirationCheckInterval = value;
            }
        }

    }
}
