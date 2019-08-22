using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Realm
{
	internal class RealmStorageConnection : JobStorageConnection
    {
	    private readonly IRealmDbContext _realmDbContext;
        private readonly RealmJobStorage _storage;
        private readonly RealmJobQueue _jobQueue;

        public RealmStorageConnection(
            RealmJobStorage storage,
            IJobQueueSemaphore jobQueueSemaphore)
	    {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _realmDbContext = storage.GetDbContext();
            _jobQueue = new RealmJobQueue(
                storage,
                jobQueueSemaphore ?? throw new ArgumentNullException(nameof(jobQueueSemaphore)));
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
	    {
		    return new RealmWriteOnlyTransaction(_realmDbContext);
	    }

	    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
	    {
		   return new RealmDistributedLock(resource, timeout, _realmDbContext, _storage.Options);
	    }

	    public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
	    {
            string jobId;

            using (var transaction = new RealmWriteOnlyTransaction(_realmDbContext))
            {
                jobId = transaction.CreateExpiredJob(job, parameters, createdAt, expireIn);
                transaction.Commit();
            }

            return jobId;
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));
            return _jobQueue.Dequeue(queues, cancellationToken);
        }


        public override void SetJobParameter(string id, string name, string value)
	    {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));
            using (var transaction = new RealmWriteOnlyTransaction(_realmDbContext))
            {
                transaction.SetJobParameter(id, name, value);
                transaction.Commit();
            }
        }

	    public override string GetJobParameter(string id, string name)
	    {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));
            using (var realm = _realmDbContext.GetRealm())
            {
                var jobDto = realm.Find<JobDto>(id);
                var param = jobDto.Parameters.Where(_ => _.Key == name).FirstOrDefault();
                return param?.Value;
            }
        }
        [CanBeNull]
        public override JobData GetJobData(string jobId)
	    {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            using (var realm = _realmDbContext.GetRealm())
            {
                var jobData = realm.Find<JobDto>(jobId);

                if (jobData == null)
                {
                    return null;
                }

                var invocationData = SerializationHelper.Deserialize<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.DeserializeJob();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = jobData.StateName,
                    CreatedAt = jobData.Created.DateTime,
                    LoadException = loadException
                };
            }
        }
        [CanBeNull]
        public override StateData GetStateData(string jobId)
	    {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            using (var realm = _realmDbContext.GetRealm())
            {
                var job = realm.Find<JobDto>(jobId);

                if (job == null)
                {
                    return null;
                }

                var state = job.StateHistory.LastOrDefault();

                if (state == null)
                {
                    return null;
                }

                return new StateData
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = state.Data.ToDictionary(x => x.Key, x => x.Value)
                };
            }

        }

	    public override void AnnounceServer(string serverId, ServerContext context)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }
            using (var realm = _realmDbContext.GetRealm())
            {
                realm.Write(() =>
                {
                    var server = new ServerDto
                    {
                        Id = serverId,
                        WorkerCount = context.WorkerCount,
                        StartedAt = DateTime.UtcNow,
                        LastHeartbeat = DateTime.UtcNow
                    };
                    ((List<string>)server.Queues).AddRange(context.Queues);
                    realm.Add(server, update: true);
                });
            }
        }

	    public override void RemoveServer(string serverId)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }
            using (var realm = _realmDbContext.GetRealm())
            {
                realm.Write(() =>
                {
                    var server = realm.Find<ServerDto>(serverId);
                    realm.Remove(server);
                });
            }
        }

	    public override void Heartbeat(string serverId)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }
            using (var realm = _realmDbContext.GetRealm())
            {
                realm.Write(() =>
                {
                    var servers = realm.All<ServerDto>()
                    .Where(d => d.Id == serverId);
                    foreach (var server in servers)
                    {
                        server.LastHeartbeat = DateTime.UtcNow;
                    }
                });
            }
        }

	    public override int RemoveTimedOutServers(TimeSpan timeOut)
	    {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }
            DateTime cutoff = DateTime.UtcNow.Add(timeOut.Negate());
            int deletedServerCount = 0;
            using (var realm = _realmDbContext.GetRealm())
            {
                realm.Write(() =>
                {
                    var servers = realm.All<ServerDto>()
                   .Where(_ => _.LastHeartbeat < cutoff);
                    foreach (var server in servers)
                    {
                        realm.Remove(server);
                        deletedServerCount++;
                    }
                });
            }
            return deletedServerCount;
        }
        // Set operations
        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var results = realm.All<SetDto>()
                    .Where(_ => _.Key == key)
                    .OrderByDescending(_ => _.Created)
                    .ToList()
                    .Select(_ => _.Value)
                    .ToList();
                return results;
            }
        }
        [NotNull]
        public override HashSet<string> GetAllItemsFromSet(string key)
	    {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<SetDto>()
                .Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.Value);
                return new HashSet<string>(result);
            }
        }
        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var count = realm.All<SetDto>().Where(_ => _.Key == key).Count();
                return count;
            }
        }
        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
	    {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (toScore < fromScore)
            {
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            }

            return GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

        }
        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            //if (key == null) throw new ArgumentNullException(nameof(key));
            if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.", nameof(toScore));
            using (var realm = _realmDbContext.GetRealm())
            {
                var sets = realm.All<SetDto>()
                .Where(_ => _.Key == key);
                return sets
                    .Where(_ =>
                    _.Score >= fromScore && _.Score <= toScore
                    )
                    .OrderBy(_ => _.Score)
                    .ToList()
                    .Take(count)
                    .Select(_ => _.Value)
                    .ToList();
            }
        }
        // hash operations
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
	    {

            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));
            using (var realm = _realmDbContext.GetRealm())
            {
                realm.Write(() =>
                {
                    HashDto hash = new HashDto
                    {
                        Key = key,
                        Created = DateTimeOffset.UtcNow
                    };
                    foreach (var field in keyValuePairs)
                    {
                        hash.Fields.Add(new FieldDto { Key = field.Key, Value = field.Value });
                    }
                    realm.Add(hash);
                });
            }
        }
        [CanBeNull]
        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
	    {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<HashDto>()
                .Where(_ => _.Key == key)
                .ToList()
                .SelectMany(_ => _.Fields)
                .ToList()
                .GroupBy(_ => _.Key, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(_ => _.Last().Key, _ => _.Last().Value);
                return result.Count != 0 ? result : null;
            }
        }
        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<SetDto>().Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.ExpireAt)
                .Min();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            }

        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<CounterDto>()
                .Where(_ => _.Key == key)
                .ToList()
                .Select(_ => (long)_.Value)
                .Sum();

                return result;
            }
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<HashDto>().Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.ExpireAt)
                .Min();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTimeOffset.UtcNow;
            }
        }
        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<HashDto>()
                .Where(_ => _.Key == key)
                .Count();
                return (long)result;
            }
            
        }
        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));
            string result = string.Empty;
            using (var realm = _realmDbContext.GetRealm())
            {
                var hashList = realm.All<HashDto>()
                .Where(_ => _.Key == key)
                .ToList();
                foreach (var hash in hashList)
                {
                    foreach (var field in hash.Fields.Where(_ => _.Key == name))
                    {
                        result += field.Value;
                    }
                }
                return result;
            }
        }
        
        //List

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<ListDto>()
                .Where(_ => _.Key == key)
                .Count();
                return (long)result;
            }
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var result = realm.All<ListDto>().Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.ExpireAt)
                .Min();
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTimeOffset.UtcNow;
            }
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var results = realm.All<ListDto>()
                    .Where(_ => _.Key == key)
                    .OrderByDescending(_ => _.Created)
                    .ToList()
                    .SelectMany(_ => _.Values)
                    .ToList();
                return results;
            }
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using (var realm = _realmDbContext.GetRealm())
            {
                var results = realm.All<ListDto>()
                    .Where(_ => _.Key == key)
                    .OrderByDescending(_ => _.Created)
                    .ToList()
                    .SelectMany(_ => _.Values)
                    .ToList();
                return results;
            }
        }

    }
}
