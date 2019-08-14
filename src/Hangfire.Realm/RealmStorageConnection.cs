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
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));
            if (value == null) throw new ArgumentNullException(nameof(value));
            _realmDbContext.Write(realm =>
            {
                var jobDto = realm.Find<JobDto>(id);
                jobDto.Parameters.Add(new ParameterDto
                {
                    Key = name,
                    Value = value
                });
            });

        }

	    public override string GetJobParameter(string id, string name)
	    {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));
            string value = string.Empty;
            var realm = _realmDbContext.GetRealm();
            var jobDto = realm.Find<JobDto>(id);
            var param = jobDto.Parameters.Where(_ => _.Key == name).FirstOrDefault();
            return param?.Value;
        }
        [CanBeNull]
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
        [CanBeNull]
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

            _realmDbContext.Write(realm =>
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
        // Set operations
        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            List<string> result = new List<string>();

            var realm = _realmDbContext.GetRealm();
            var sets = realm.All<SetDto>()
                    .Where(_ => _.Key == key)
                    .OrderByDescending(_ => _.Created)
                    .ToArray();

            for (var i = startingFrom; i < startingFrom + endingAt && i < sets.Count(); i++)
            {
                result.Add(sets[i].Value);
            }
            return result;
        }
        [NotNull]
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

            return GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

        }
        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (count <= 0) throw new ArgumentException("The value must be a positive number", nameof(count));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.", nameof(toScore));
            List<string> result = new List<string>();
            var realm = _realmDbContext.GetRealm();
            var sets = realm.All<SetDto>()
                .Where(_ => _.Key.StartsWith("key"))
                .Where(_ => _.Score >= fromScore)
                .Where(_ => _.Score <= toScore)
                .OrderBy(_ => _.Score).ToArray();
            for (var i = 0; i < count && i < sets.Count(); i++)
            {
                result.Add(sets[i].Value);
            }
            return result;

        }
        // hash operations
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
	    {

            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));
            var realm = _realmDbContext.GetRealm();
            var query = realm.All<HashDto>();

            throw new NotImplementedException();

            //            var sql =
            //$@";merge [{_storage.SchemaName}].Hash with (holdlock, forceseek) as Target
            //using (VALUES (@key, @field, @value)) as Source ([Key], Field, Value)
            //on Target.[Key] = Source.[Key] and Target.Field = Source.Field
            //when matched then update set Value = Source.Value
            //when not matched then insert ([Key], Field, Value) values (Source.[Key], Source.Field, Source.Value);";

            //            var lockResourceKey = $"{_storage.SchemaName}:Hash:Lock";

            //            _storage.UseTransaction(_dedicatedConnection, (connection, transaction) =>
            //            {
            //                using (var commandBatch = new SqlCommandBatch(preferBatching: _storage.CommandBatchMaxTimeout.HasValue))
            //                {
            //                    if (!_storage.Options.DisableGlobalLocks)
            //                    {
            //                        commandBatch.Append(
            //                            "SET XACT_ABORT ON;exec sp_getapplock @Resource=@resource, @LockMode=N'Exclusive', @LockOwner=N'Transaction', @LockTimeout=-1;",
            //                            new SqlParameter("@resource", lockResourceKey));
            //                    }

            //                    foreach (var keyValuePair in keyValuePairs)
            //                    {
            //                        commandBatch.Append(sql,
            //                            new SqlParameter("@key", key),
            //                            new SqlParameter("@field", keyValuePair.Key),
            //                            new SqlParameter("@value", (object)keyValuePair.Value ?? DBNull.Value));
            //                    }

            //                    if (!_storage.Options.DisableGlobalLocks)
            //                    {
            //                        commandBatch.Append(
            //                            "exec sp_releaseapplock @Resource=@resource, @LockOwner=N'Transaction';",
            //                            new SqlParameter("@resource", lockResourceKey));
            //                    }

            //                    commandBatch.Connection = connection;
            //                    commandBatch.Transaction = transaction;
            //                    commandBatch.CommandTimeout = _storage.CommandTimeout;
            //                    commandBatch.CommandBatchMaxTimeout = _storage.CommandBatchMaxTimeout;

            //                    commandBatch.ExecuteNonQuery();
            //                }
            //            });
        }
        [CanBeNull]
        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
	    {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _realmDbContext.GetRealm();
            var hashList = realm.All<HashDto>()
                .Where(_ => _.Key == key)
                .ToList();
            var result = new Dictionary<string, string>();
            //TODO: This is wonky
            foreach (var hash in hashList)
            {
                foreach (var field in hash.Fields)
                {
                    result.Add(field.Key, field.Value);
                }
                
            }
            return result.Count != 0 ? result : null;

        }
        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            throw new NotImplementedException();

        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            throw new NotImplementedException();
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            throw new NotImplementedException();
        }

        public override string GetValueFromHash(string key, string name)
        {
            throw new NotImplementedException();
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            throw new NotImplementedException();
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            throw new NotImplementedException();
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            throw new NotImplementedException();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            throw new NotImplementedException();
        }

    }
}
