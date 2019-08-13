using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Storage;
using System;

namespace Hangfire.Realm
{
    public sealed class RealmFetchedJob : IFetchedJob
    {
        private static readonly ILog Logger = LogProvider.For<RealmFetchedJob>();
        private readonly IRealmDbContext _dbContext;
        private readonly string _id;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public RealmFetchedJob(IRealmDbContext dbContext, string id, string jobId, string queue)
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _id = id ?? throw new ArgumentNullException(nameof(id));
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));
            
        }

        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            var realm = _dbContext.GetRealm();
            var queuedJob = realm.Find<JobQueueDto>(_id);
            if (queuedJob != null)
            {
                _dbContext.Write(r => {
                r.Remove(queuedJob);
                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
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
                _dbContext.Write(r => 
                {
                var notification = NotificationDto.JobEnqueued(Queue);
                queuedJob.FetchedAt = null;
                r.Add<NotificationDto>(notification);
                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"Requeue job '{JobId}' from queue '{Queue}'");
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
