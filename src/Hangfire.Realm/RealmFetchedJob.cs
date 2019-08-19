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
        private readonly RealmJobStorage _storage;
        private readonly string _id;
        private readonly Timer _timer;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public RealmFetchedJob(
            [NotNull] RealmJobStorage storage,
            [NotNull] IRealmDbContext dbContext,
            [NotNull] string id,
            [NotNull] string jobId,
            [NotNull] string queue,
            [NotNull] DateTimeOffset? fetchedAt)
        {
            if (fetchedAt == null)
            {
                throw new ArgumentNullException(nameof(fetchedAt));
            }

            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
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
                        FetchedAt = DateTimeOffset.UtcNow;
                        queuedJob.FetchedAt = FetchedAt;
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
            var realm = _dbContext.GetRealm();
            var queuedJob = realm.Find<JobQueueDto>(_id);
            if (queuedJob != null)
            {
                realm.Write(() => {
                realm.Remove(queuedJob);
                if (_logger.IsTraceEnabled())
                {
                    _logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
                }
            });
            }
            _removedFromQueue = true;
        }

        public void Requeue()
        {
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
            _requeued = true;
        }

        public void Dispose()
        {
            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Requeue();
            }
            _disposed = true;
        }
    }
}
