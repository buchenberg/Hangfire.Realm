using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Realm
{
	internal class RealmStorageConnection : JobStorageConnection
    {
	    private readonly IRealmDbContext _realmDbContext;
        private readonly RealmJobStorageOptions _storageOptions;
        private readonly RealmJobStorage _storage;

        public RealmStorageConnection(
            IRealmDbContext realmDbContext, 
            RealmJobStorageOptions storageOptions)
	    {
		    _realmDbContext = realmDbContext;
		    _storageOptions = storageOptions;
            _storage = new RealmJobStorage(storageOptions);
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
	    {
		    return new RealmWriteOnlyTransaction(_realmDbContext);
	    }

	    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
	    {
		    return new RealmDistributedLock(resource, timeout, _realmDbContext, _storageOptions);
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

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {String.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }

            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }


        public override void SetJobParameter(string id, string name, string value)
	    {
		    throw new NotImplementedException();
	    }

	    public override string GetJobParameter(string id, string name)
	    {
		    throw new NotImplementedException();
	    }

	    public override JobData GetJobData(string jobId)
	    {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            var realm = _realmDbContext.GetRealm();
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

	    public override StateData GetStateData(string jobId)
	    {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            var realm = _realmDbContext.GetRealm();
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
                Data =   state.Data.ToDictionary(x => x.Key, x => x.Value)
            };
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

            _realmDbContext.Write(r =>
            {
                var server = new ServerDto
                {
                    Id = serverId,
                    WorkerCount = context.WorkerCount,
                    StartedAt = DateTime.UtcNow,
                    LastHeartbeat = DateTime.UtcNow
                };
                ((List<string>)server.Queues).AddRange(context.Queues);
                r.Add(server, update: true);
            });
        }

	    public override void RemoveServer(string serverId)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }
            _realmDbContext.Write(realm =>
            {
                var server = realm.Find<ServerDto>(serverId);
                realm.Remove(server);
            });
        }

	    public override void Heartbeat(string serverId)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }
            _realmDbContext.Write(realm =>
            {
                var servers = realm.All<ServerDto>()
                .Where(d => d.Id == serverId);
                foreach (var server in servers)
                {
                    server.LastHeartbeat = DateTime.UtcNow;
                }
            });
        }

	    public override int RemoveTimedOutServers(TimeSpan timeOut)
	    {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }
            DateTime cutoff = DateTime.UtcNow.Add(timeOut.Negate());
            int deletedServerCount = 0;
            _realmDbContext.Write(realm =>
            {
                var servers = realm.All<ServerDto>()
               .Where(_ => _.LastHeartbeat < cutoff);
                foreach (var server in servers)
                {
                    realm.Remove(server);
                    deletedServerCount++;
                }
            });
            return deletedServerCount;
        }

	    public override HashSet<string> GetAllItemsFromSet(string key)
	    {
		    throw new NotImplementedException();
	    }
        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _realmDbContext.GetRealm();
            var count = realm.All<SetDto>().Where(_ => _.Key == key).Count();
            return count;
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
            var realm = _realmDbContext.GetRealm();
            var set = realm.All<SetDto>()
                .Where(_ => _.Key.StartsWith("key"))
                .Where(_ => _.Score >= fromScore)
                .Where(_ => _.Score <= toScore)
                .OrderBy(_ => _.Score)
                .FirstOrDefault();

            var value = set?.Value;

            return value;

        }

	    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
	    {
		    throw new NotImplementedException();
	    }

	    public override Dictionary<string, string> GetAllEntriesFromHash(string key)
	    {
		    throw new NotImplementedException();
	    }
    }
}
