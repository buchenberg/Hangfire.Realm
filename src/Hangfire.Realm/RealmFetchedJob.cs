using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Storage;
using System;

namespace Hangfire.Realm
{
    public sealed class RealmFetchedJob : IFetchedJob
    {
        private static readonly ILog Logger = LogProvider.For<RealmFetchedJob>();
        private readonly Realms.Realm _realm;
        private readonly string _id;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public RealmFetchedJob(IRealmDbContext dbContext, string id, string jobId, string queue)
        {
            _realm = dbContext.GetRealm();
            _id = id ?? throw new ArgumentNullException(nameof(id));
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            
        }

        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            var queuedJob = _realm.Find<QueuedJobDto>(_id);
            _realm.Write(() => _realm.Remove(queuedJob));
            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"Remove job '{JobId}' from queue '{Queue}'");
            }
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            var queuedJob = _realm.Find<QueuedJobDto>(_id);
            var notification = NotificationDto.JobEnqueued(Queue);
            _realm.Write(() =>
            {
                queuedJob.FetchedAt = null;
                _realm.Add<NotificationDto>(notification);
            });

            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
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
            _realm.Dispose();
            _disposed = true;
        }
    }
}
