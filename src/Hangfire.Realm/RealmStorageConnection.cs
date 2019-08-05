using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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

	    public RealmStorageConnection(IRealmDbContext realmDbContext, RealmJobStorageOptions _storageOptions)
	    {
		    _realmDbContext = realmDbContext;
		    this._storageOptions = _storageOptions;
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
		    throw new NotImplementedException();
	    }

	    public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
	    {
		    throw new NotImplementedException();
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
            var jobData = realm.All<JobDto>()
                .First(d => d.Id == jobId);

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
            var job = realm.All<JobDto>()
                .First(d => d.Id == jobId);

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

            var realm = _realmDbContext.GetRealm();

            realm.Write(() =>
            {
                var server = new ServerDto
                {
                    Id = serverId,
                    WorkerCount = context.WorkerCount,
                    StartedAt = DateTime.UtcNow,
                    LastHeartbeat = DateTime.UtcNow
                };

                foreach (var queue in context.Queues)
                {
                    server.Queues.Add(queue);
                };

                realm.Add(server, update: true);
            });
        }

	    public override void RemoveServer(string serverId)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }
            var realm = _realmDbContext.GetRealm();
            var server = realm.All<ServerDto>()
                .First(d => d.Id == serverId);
            using (var trans = realm.BeginWrite())
            {
                realm.Remove(server);
                trans.Commit();
            }

        }

	    public override void Heartbeat(string serverId)
	    {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }
            var realm = _realmDbContext.GetRealm();
            var servers = realm.All<ServerDto>()
                .Where(d => d.Id == serverId);
            using (var transaction = realm.BeginWrite())
            {
                foreach (var server in servers)
                {
                    server.LastHeartbeat = DateTime.UtcNow;
                }

                transaction.Commit();
            }
        }

	    public override int RemoveTimedOutServers(TimeSpan timeOut)
	    {
		    throw new NotImplementedException();
	    }

	    public override HashSet<string> GetAllItemsFromSet(string key)
	    {
		    throw new NotImplementedException();
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

            var result = realm.All<SetDto>()
                .Where(_ => _.Key.Contains("key"))
                .Where(_ => _.Score >= fromScore)
                .Where(_ => _.Score <= toScore)
                .OrderBy(_ => _.Score)
                .FirstOrDefault().Value;

            return result;
                
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
