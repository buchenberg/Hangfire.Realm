using Hangfire.Annotations;
using Hangfire.Realm.DAL;
using Hangfire.Realm.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Hangfire.Realm
{
    public class RealmJobQueueMonitoringApi
    {

        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly object _cacheLock = new object();

        private List<string> _queuesCache = new List<string>();
        private readonly RealmJobStorage _storage;
        private Stopwatch _cacheUpdated;
        public RealmJobQueueMonitoringApi([NotNull] RealmJobStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public IEnumerable<string> GetQueues()
        {
            lock (_cacheLock)
            {
                if (_queuesCache.Count != 0 && _cacheUpdated.Elapsed <= QueuesCacheTimeout)
                    return _queuesCache.ToList();
                using (var realm = _storage.GetRealm())
                {
                    _queuesCache = realm.All<JobQueueDto>()
                        .Select(q => q.Queue)
                        .Distinct()
                        .ToList();
                }
                    
                _cacheUpdated = Stopwatch.StartNew();

                return _queuesCache;
            }
        }

    }
}
