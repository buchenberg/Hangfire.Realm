using System;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Realm.Extensions;
using Hangfire.Realm.Models;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    public class RealmJobQueue
    {
        // This is an optimization that helps to overcome the polling delay, when
        // both client and server reside in the same process. Everything is working
        // without this event, but it helps to reduce the delays in processing.
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(false);
        private static readonly ILog Logger = LogProvider.For<RealmJobQueue>();
        private readonly IRealmDbContext _dbContext;
        private readonly RealmJobStorage _storage;
        private readonly RealmJobStorageOptions _options;
        private readonly IJobQueueSemaphore _semaphore;

        public RealmJobQueue([NotNull] RealmJobStorage storage, RealmJobStorageOptions options, IJobQueueSemaphore semaphore)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _semaphore = semaphore ?? throw new ArgumentNullException(nameof(semaphore));
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

                if (_semaphore.WaitAny(queues, cancellationToken, _options.QueuePollInterval, out var queue))
                {
                    fetchedJob = TryGetEnqueuedJob(queue, cancellationToken);
                }

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
            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                //WaitHandle.WaitAny(new WaitHandle[] { cancellationEvent.WaitHandle, NewItemInQueueEvent }, _options.QueuePollInterval);
                //cancellationToken.ThrowIfCancellationRequested();
                foreach (var queue in queues)
                {
                    var fetchedJob = TryGetEnqueuedJob(queue, cancellationToken);
                    if (fetchedJob == null)
                    {
                        continue;
                    }
                    _semaphore.WaitNonBlock(queue);
                    return fetchedJob;
                }
            }
            

            return null;
        }

        private RealmFetchedJob TryGetEnqueuedJob(string queue, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var timeout = DateTimeOffset.UtcNow.AddSeconds(_options.SlidingInvisibilityTimeout.Value.TotalSeconds);
            RealmFetchedJob fetchedJob = null;
            var realm = _dbContext.GetRealm();
            realm.Write(() =>
            {
               var enqueuedJobs = realm.All<JobQueueDto>();
               var job = enqueuedJobs
                .Where(_ => _.Queue == queue)
                .OrderBy(_ => _.Created)
                .Where(_ => _.FetchedAt == null || _.FetchedAt <= timeout)
                .FirstOrDefault();
                if (job != null)
                {
                    var fetchedAt = DateTimeOffset.UtcNow;
                    job.FetchedAt = fetchedAt;
                    fetchedJob = new RealmFetchedJob(_storage, job.Id, job.JobId, job.Queue, fetchedAt);
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
