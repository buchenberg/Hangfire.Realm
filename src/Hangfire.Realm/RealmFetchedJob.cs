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

        private readonly object _syncRoot = new object();
        private readonly RealmJobStorage _storage;
        private readonly string _id;
        private readonly Timer _timer;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _queued;

        public static RealmFetchedJob CreateInstance(RealmJobStorage storage, string id, string jobId, string queue, DateTimeOffset? fetchedAt)
        {
            return new RealmFetchedJob(storage, id, jobId, queue, fetchedAt);
        }

        private static readonly ILog Logger = LogProvider.For<RealmFetchedJob>();


        private RealmFetchedJob(
            [NotNull] RealmJobStorage storage,
            [NotNull] string id,
            [NotNull] string jobId,
            [NotNull] string queue,
            [NotNull] DateTimeOffset? fetchedAt)
        {

            if (fetchedAt == null)
            {
                throw new ArgumentNullException(nameof(fetchedAt));
            }
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
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

                if (_queued || _removedFromQueue) return;

                try
                {
                    var realm = _storage.GetRealm();
                    realm.Write(() =>
                    {
                        var queuedJob = realm.Find<JobQueueDto>(_id);
                        if (queuedJob == null) return;
                        FetchedAt = DateTimeOffset.UtcNow;
                        queuedJob.FetchedAt = FetchedAt;

                    });

                    if (!FetchedAt.HasValue)
                    {
                        Logger.Warn($"Background job identifier '{JobId}' was fetched by another worker, will not execute keep alive.");
                    }

                    Logger.Trace($"Keep-alive query for message {_id} sent");
                }
                catch (Exception ex)
                {
                    Logger.DebugException($"Unable to execute keep-alive query for message {_id}", ex);
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
                var realm = _storage.GetRealm();
                realm.Write(() =>
                {
                    var queuedJob = realm.Find<JobQueueDto>(_id);
                    if (queuedJob != null)
                    {

                        realm.Remove(queuedJob);
                        if (Logger.IsTraceEnabled())
                        {
                            Logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
                        }

                    }
                });
                _removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            lock (_syncRoot)
            {
                if (!FetchedAt.HasValue) return;
                var realm = _storage.GetRealm();
                realm.Write(() =>
                     {
                         var queuedJob = realm.Find<JobQueueDto>(_id);
                         if (queuedJob == null) return;
                         var notification = new NotificationDto
                         {
                             Type = NotificationType.JobEnqueued.ToString(),
                             Value = Queue
                         };
                         queuedJob.FetchedAt = null;
                         realm.Add<NotificationDto>(notification);
                         if (Logger.IsTraceEnabled())
                         {
                             Logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
                         }
                     });
                FetchedAt = null;
                _queued = true;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _timer?.Dispose();

            lock (_syncRoot)
            {
                if (!_removedFromQueue && !_queued)
                {
                    Requeue();
                }
            }
        }
    }
}
