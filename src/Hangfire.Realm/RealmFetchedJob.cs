using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Storage;
using System;
using System.Threading;

namespace Hangfire.Realm
{
    public sealed class RealmFetchedJob : IFetchedJob
    {
        private static readonly ILog _logger = LogProvider.For<RealmFetchedJob>();
        private readonly object _syncRoot = new object();
        private readonly IRealmDbContext _dbContext;
        private readonly string _id;
        private readonly Timer _timer;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public RealmFetchedJob(
            [NotNull] RealmJobStorage storage,
            [NotNull] string id,
            [NotNull] string jobId,
            [NotNull] string queue,
            [NotNull] DateTimeOffset? fetchedAt)
        {
            if (storage == null)
            {
                throw new ArgumentNullException(nameof(storage));
            }
            if (fetchedAt == null)
            {
                throw new ArgumentNullException(nameof(fetchedAt));
            }

            _dbContext = storage.GetDbContext() ?? throw new ArgumentNullException(nameof(storage));
            _id = id ?? throw new ArgumentNullException(nameof(id));
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            FetchedAt = fetchedAt.Value;

            if (storage.SlidingInvisibilityTimeout.HasValue)
            {
                var keepAliveInterval =
                    TimeSpan.FromSeconds(storage.SlidingInvisibilityTimeout.Value.TotalSeconds / 5);
                _timer = new Timer(ExecuteKeepAliveQuery, null, keepAliveInterval, keepAliveInterval);
            }

        }

        private void ExecuteKeepAliveQuery(object obj)
        {
            lock (_syncRoot)
            {
                if (!FetchedAt.HasValue) return;

                if (_requeued || _removedFromQueue) return;

                try
                {
                    var realm = _dbContext.GetRealm();
                    realm.Write(() =>
                    {
                        var queuedJob = realm.Find<JobQueueDto>(_id);
                        if (queuedJob != null)
                        {
                            FetchedAt = DateTimeOffset.UtcNow;
                            queuedJob.FetchedAt = FetchedAt;
                        }
                        
                    });

                    if (!FetchedAt.HasValue)
                    {
                        _logger.Warn($"Background job identifier '{JobId}' was fetched by another worker, will not execute keep alive.");
                    }

                    _logger.Trace($"Keep-alive query for message {_id} sent");
                }
                catch (Exception ex)
                {
                    _logger.DebugException($"Unable to execute keep-alive query for message {_id}", ex);
                }
            }
        }

        public string JobId { get; }
        public string Queue { get; }

        internal DateTimeOffset? FetchedAt { get; private set; }

        public void RemoveFromQueue()
        {
            lock (_syncRoot)
            {
                if (!FetchedAt.HasValue) return;
                var realm = _dbContext.GetRealm();
                var queuedJob = realm.Find<JobQueueDto>(_id);
                if (queuedJob != null)
                {
                    realm.Write(() =>
                    {
                        realm.Remove(queuedJob);
                        if (_logger.IsTraceEnabled())
                        {
                            _logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
                        }
                    });
                }
                _removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            lock (_syncRoot)
            {
                if (!FetchedAt.HasValue) return;
                var realm = _dbContext.GetRealm();
                var queuedJob = realm.Find<JobQueueDto>(_id);
                if (queuedJob != null)
                {
                    realm.Write(() => 
                    {
                        var notification = NotificationDto.JobEnqueued(Queue);
                        queuedJob.FetchedAt = null;
                        realm.Add<NotificationDto>(notification);
                        if (_logger.IsTraceEnabled())
                        {
                            _logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
                    }
                });
                }
                FetchedAt = null;
                _requeued = true;
                _requeued = true;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _timer?.Dispose();

            lock (_syncRoot)
            {
                if (!_removedFromQueue && !_requeued)
                {
                    Requeue();
                }
            }
        }
    }
}
