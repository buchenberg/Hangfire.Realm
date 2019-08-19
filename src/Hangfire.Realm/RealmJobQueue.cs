using System;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Realm.Extensions;
using Hangfire.Realm.Models;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    public class RealmJobQueue
    {
        private static readonly ILog Logger = LogProvider.For<RealmJobQueue>();
        private readonly IRealmDbContext _dbContext;
        //private readonly DateTime _invisibilityTimeout;
        //private readonly RealmJobStorageOptions _options;

        public RealmJobQueue([NotNull] RealmJobStorage storage)
        {
            //_options = options ?? throw new ArgumentNullException(nameof(options));
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            _dbContext = storage.GetDbContext();
            
        }


        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
            }

            RealmFetchedJob fetchedJob = null;

            while (fetchedJob == null)
            {

                cancellationToken.ThrowIfCancellationRequested();
                fetchedJob = TryAllQueues(queues, cancellationToken);

                if (fetchedJob != null) return fetchedJob;

            }

            return fetchedJob;
        }

        public void Enqueue(string queue, string jobId)
        {
            var realm = _dbContext.GetRealm();
            realm.Add(new JobQueueDto { Queue = queue, JobId = jobId });
        }

        private RealmFetchedJob TryAllQueues(string[] queues, CancellationToken cancellationToken)
        {
            foreach (var queue in queues)
            {
                var fetchedJob = TryGetEnqueuedJob(queue, cancellationToken);
                if (fetchedJob == null)
                {
                    continue;
                }
                return fetchedJob;
            }

            return null;
        }

        private RealmFetchedJob TryGetEnqueuedJob(string queue, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RealmFetchedJob fetchedJob = null;
            var realm = _dbContext.GetRealm();
            realm.Write(() =>
            {
               var enqueuedJobs = realm.All<JobQueueDto>();
               var job = enqueuedJobs
                .Where(_ => _.Queue == queue)
                .OrderBy(_ => _.Created)
                //.Where(_ => _.FetchedAt < _invisibilityTimeout)
                .FirstOrDefault();
                if (job != null)
                {
                    job.FetchedAt = DateTimeOffset.UtcNow;
                    fetchedJob = new RealmFetchedJob(_dbContext, job.Id, job.JobId, job.Queue);
                    if (Logger.IsTraceEnabled())
                    {
                        Logger.Trace($"Fetched job {job.JobId} from '{queue}' Thread[{Thread.CurrentThread.ManagedThreadId}]");
                    }
                }
                
            });
            return fetchedJob;

        }
    }

}
