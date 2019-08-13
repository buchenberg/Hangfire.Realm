using System;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    public class RealmJobQueue : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<RealmJobQueue>();

        //private readonly RealmJobStorageOptions _storageOptions;
        //private readonly IJobQueueSemaphore _semaphore;

        private readonly IRealmDbContext _dbContext;
        private Realms.Realm _db;
        private readonly DateTime _invisibilityTimeout;

        private readonly RealmJobStorage _storage;
        private readonly RealmJobStorageOptions _options;


        //public RealmJobQueue(IRealmDbContext dbContext)
        //{
        //    _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        //    _db = _dbContext.GetRealm();
        //}

        public RealmJobQueue([NotNull] RealmJobStorage storage, RealmJobStorageOptions options)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _dbContext = storage.GetDbContext();
        }

        public void Dispose()
        {
            if (_db != null)
            {
                _db.Dispose();
                _db = null;
            }
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

                //TODO
                //if (_semaphore.WaitAny(queues, cancellationToken, _storageOptions.QueuePollInterval, out var queue))
                //{
                //    fetchedJob = TryGetEnqueuedJob(queue, cancellationToken);
                //}
            }

            return fetchedJob;
        }


        public void Enqueue(string queue, string jobId)
        {
            _dbContext.Write(realm => {
                realm.Add(new JobQueueDto { Id = Guid.NewGuid().ToString(), Created = DateTimeOffset.UtcNow, Queue = queue, JobId = jobId });
             });
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
            var enqueuedJobs = _db.All<JobQueueDto>();

            var fetchedJob = enqueuedJobs
                .Where(_ => _.Queue == queue)
                //.Where(_ => _.FetchedAt < _invisibilityTimeout)
                .FirstOrDefault();


            if (fetchedJob == null)
            {
                return null;
            }

            _dbContext.Write(realm => {
                realm.Write(() =>
                {
                    fetchedJob.FetchedAt = DateTime.UtcNow;
                });
            });

            if (Logger.IsTraceEnabled())
            {
                Logger.Trace($"Fetched job {fetchedJob.JobId} from '{queue}' Thread[{Thread.CurrentThread.ManagedThreadId}]");
            }
            return new RealmFetchedJob(_dbContext, fetchedJob.Id, fetchedJob.JobId, fetchedJob.Queue);

        }
    }

}
