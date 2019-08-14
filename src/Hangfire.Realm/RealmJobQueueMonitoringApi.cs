using Hangfire.Annotations;
using Hangfire.Realm.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Hangfire.Realm
{
    public class RealmJobQueueMonitoringApi
    {

        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly object _cacheLock = new object();

        private List<string> _queuesCache = new List<string>();
        private readonly IRealmDbContext _context;
        private Stopwatch _cacheUpdated;
        public RealmJobQueueMonitoringApi([NotNull] RealmJobStorage storage)
        {
            if (storage == null)
            {
                throw new ArgumentNullException(nameof(storage));
            }

            _context = storage.GetDbContext();
        }

        public IEnumerable<string> GetQueues()
        {

            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Elapsed > QueuesCacheTimeout)
                {
                    using (var realm = _context.GetRealm())
                    {
                        _queuesCache = realm.All<JobQueueDto>().Select(q => q.Queue).Distinct().ToList();
                        _cacheUpdated = Stopwatch.StartNew();
                    }                   
                }

                return _queuesCache.ToList();
            }
        }

    }
}
