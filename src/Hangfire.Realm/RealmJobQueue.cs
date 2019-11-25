using System;
using System.Collections.Generic;
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
        private readonly RealmJobStorage _storage;

        public RealmJobQueue([NotNull] RealmJobStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }
        public void Enqueue(string queue, string jobId)
        {
            try
            {
                var realm = _storage.GetRealm();
                realm.Write(() =>
                {
                    realm.Add(new JobQueueDto { Queue = queue, JobId = jobId });
                });
            }
            catch (Exception e)
            {
                Logger.ErrorException($"Error adding job {jobId} to the {queue} queue.", e);
                throw;
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

            var pollInterval = _storage.Options.QueuePollInterval > TimeSpan.Zero
                ? _storage.Options.QueuePollInterval
                : TimeSpan.FromSeconds(1);
            var timeout = DateTimeOffset.UtcNow.AddSeconds((int)_storage.Options.SlidingInvisibilityTimeout.Value.Negate().TotalSeconds);
            RealmFetchedJob fetched = null;

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var realm = _storage.GetRealm();
                    realm.Write(() =>
                    {
                        List<JobQueueDto> jobs = new List<JobQueueDto>();
                        foreach (var queue in queues)
                        {
                            var jobsInQueue = realm.All<JobQueueDto>()
                           .Where(_ => (_.FetchedAt == null || _.FetchedAt < timeout))
                           .Where(_ => _.Queue == queue);
                            jobs.AddRange(jobsInQueue);
                        }
                        var job = jobs.OrderBy(_ => _.Created).FirstOrDefault();

                        if (job != null)
                        {
                            if (Logger.IsTraceEnabled())
                            {
                                Logger.Debug($"Fetched job {job.JobId} with FetchedAt {job.FetchedAt.ToString()} by Thread[{Thread.CurrentThread.ManagedThreadId}]");
                            }
                            job.FetchedAt = DateTimeOffset.UtcNow;
                            fetched = new RealmFetchedJob(_storage, job.Id, job.JobId, job.Queue, job.FetchedAt);
                        }
                    });
                    if (fetched != null)
                    {
                        break;
                    }
                    cancellationToken.WaitHandle.WaitOne(pollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                } while (true);
            }
            return fetched;
        }

    }
}


