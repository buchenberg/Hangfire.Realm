using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Realms;

namespace Hangfire.Realm
{
    public class RealmWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly RealmJobStorage _storage;
        private readonly ILog _logger;

        public RealmWriteOnlyTransaction(RealmJobStorage storage)
        {
            _logger = LogProvider.For<RealmStorageConnection>();
            _storage = storage;
        }
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var realm = _storage.GetRealm();
            var job = realm.Find<JobDto>(jobId);
            if (job == null) return;
            realm.Write(() => job.ExpireAt = DateTime.UtcNow.Add(expireIn));
        }
        public string CreateExpiredJob(
            Job job, 
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));

            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            var jobId = string.Empty;
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var invocationData = InvocationData.SerializeJob(job);

                var jobDto = new JobDto
                {
                    InvocationData = SerializationHelper.Serialize<InvocationData>(invocationData),
                    Arguments = invocationData.Arguments,
                    Created = createdAt,
                    ExpireAt = createdAt.Add(expireIn)
                };

                foreach (var param in parameters)
                {
                    jobDto.Parameters.Add(new ParameterDto(param.Key, param.Value));
                }
                
                realm.Add(jobDto);
                jobId = jobDto.Id;
            });
            return jobId;
        }

        public override void PersistJob(string jobId)
        {
            var realm = _storage.GetRealm();
            var job = realm.Find<JobDto>(jobId);
            if (job == null) return;
            realm.Write(() => job.ExpireAt = null);
        }

        public override void SetJobState(string jobId, IState state)
        {
            _logger.DebugFormat("Setting Hangfire job {0} state to {1}", jobId, state.Name);
            var realm = _storage.GetRealm();
            var job = realm.Find<JobDto>(jobId);
            if (job == null) return;
                
            
            var stateData = new StateDto
            {
                Reason = state.Reason,
                Name = state.Name
            };
            foreach (var data in state.SerializeData())
            {
                stateData.Data.Add(new StateDataDto(data));
            }
            realm.Write(() =>
            {
                job.StateName = state.Name;
                job.StateHistory.Insert(0, stateData);
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            var realm = _storage.GetRealm();
            var job = realm.Find<JobDto>(jobId);
            if (job == null) return;
            var stateData = new StateDto
            {
                Reason = state.Reason,
                Name = state.Name
            };

            foreach (var data in state.SerializeData())
            {
                stateData.Data.Add(new StateDataDto(data));
            }

            realm.Write(() => job.StateHistory.Insert(0, stateData));
        }

        #region custom internal transactions
        internal void SetJobParameter(string id, string name, string value)
        {
            if (id is null)
            {
                throw new ArgumentNullException(nameof(id));
            }
            var realm = _storage.GetRealm();
            var jobDto = realm.Find<JobDto>(id);
            realm.Write(() => jobDto.Parameters.Add(new ParameterDto(name, value)));
        }

        internal LockDto AddLock(string resource)
        {
            if (resource is null)
            {
                throw new ArgumentNullException(nameof(resource));
            }
            var realm = _storage.GetRealm();
            var lockDto = new LockDto
            {
                Resource = resource,
                ExpireAt = null
            };
            realm.Write(() => realm.Add(lockDto, update: true));
            return lockDto;
        }

        internal void AnnounceServer(string serverId, ServerContext context)
        {
            try
            {
                if (serverId == null)
                {
                    throw new ArgumentNullException(nameof(serverId));
                }

                if (context == null)
                {
                    throw new ArgumentNullException(nameof(context));
                }
                var realm = _storage.GetRealm();
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
                }
                );
            }
            catch (Exception)
            {

                throw;
            }
        }

        internal void SetLockExpiry(string resource, DateTimeOffset dateTimeOffset)
        {
            if (resource is null)
            {
                throw new ArgumentNullException(nameof(resource));
            }

            var realm = _storage.GetRealm();
            realm.Write(() => realm.Add(new LockDto
            {
                Resource = resource,
                ExpireAt = dateTimeOffset
            }, update: true));

        }

        #endregion

        public override void AddToQueue(string queue, string jobId)
        {
            var realm = _storage.GetRealm();
            realm.Write(() => realm.Add(new JobQueueDto {Queue = queue, JobId = jobId}));
        }

        public override void IncrementCounter(string key)
        {
            var realm = _storage.GetRealm();
            
            realm.Write(() => {
                var counter = realm.Find<CounterDto>(key);
                if (counter == null)
                {
                    counter = realm.Add(new CounterDto(key));
                }
                counter.Value.Increment();
            });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var counter = realm.Find<CounterDto>(key);
                if (counter == null)
                {
                    counter = realm.Add(new CounterDto(key));
                }
                counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
                counter.Value.Increment();
            });
        }

        public override void DecrementCounter(string key)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var counter = realm.Find<CounterDto>(key);
                if (counter == null)
                {
                    counter = realm.Add(new CounterDto(key));
                }
                counter.Value.Decrement();
            });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var counter = realm.Find<CounterDto>(key);
                if (counter == null)
                {
                    counter = realm.Add(new CounterDto(key));
                }
                counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
                counter.Value.Decrement();
            });
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var set = realm.All<SetDto>()
                .Where(_ => _.Key == key && _.Value == value)
                .FirstOrDefault();
                if (set == null)
                {
                    realm.Add(new SetDto(key, value, score));
                    return;
                }
                set.Score = score;
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var set = realm.All<SetDto>()
                .Where(_ => _.Key == key && _.Value == value);
                if (set.Any()) realm.Remove(set.Single());
            });
            
        }

        public override void InsertToList(string key, string value)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var list = realm.Find<ListDto>(key);
                if (list == null)
                {
                    list = realm.Add(new ListDto(key));
                }
                list.Values.Add(value);
            });
            
        }

        public override void RemoveFromList(string key, string value)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var list = realm.Find<ListDto>(key);
                if (list == null) return;
                var matchingValues = list.Values.Where(_ => _ == value);
                foreach(var match in matchingValues)
                {
                    list.Values.Remove(match);
                }
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var list = realm.Find<ListDto>(key);
                if (list == null) return;
                //delete from cte where row_num not between @start and @end";
                for (int i = list.Values.Count - 1; i >= 0; i--)
                {
                    if (i < keepStartingFrom || i > keepEndingAt)
                    {
                        list.Values.RemoveAt(i);
                    }
                }
            });
        }

        //TODO: This is painfully awkward...
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var persistedHash = realm.Find<HashDto>(key);
                if (persistedHash == null)
                {
                    //simple insert of new hash
                    var hash = new HashDto(key);
                    foreach (var pair in keyValuePairs)
                    {
                        hash.Fields.Add(new FieldDto(pair.Key, pair.Value));
                    }
                    realm.Add(hash);
                }
                else
                {
                    //field updates
                    foreach (var pair in keyValuePairs)
                    {
                        var matchingFields = persistedHash.Fields.Where(_ => _.Key == pair.Key);
                        if (matchingFields.Any())
                        {
                            matchingFields.Single().Value = pair.Value;
                        }
                        else
                        {
                            persistedHash.Fields.Add(new FieldDto(pair.Key, pair.Value));
                        }
                    }
                }
            });
            
            
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var hash = realm.Find<HashDto>(key);
                if (hash == null) return;
                realm.Remove(hash);
            });
        }

        public override void Commit()
        {
            //These methods are self-commiting. Don't trust hangfire to close a transaction.
            return;
        }
        
        // New methods to support Hangfire pro feature - batches.


        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var expireAt = DateTimeOffset.UtcNow.Add(expireIn);
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                foreach (var set in realm.All<SetDto>().Where(_ => _.Key == key))
                {
                    set.ExpireAt = expireAt;
                }
            });
            
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var expireAt = DateTimeOffset.UtcNow.Add(expireIn);
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                foreach (var list in realm.All<ListDto>().Where(_ => _.Key == key))
                {
                    list.ExpireAt = expireAt;
                }
            });
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var hash = realm.Find<HashDto>(key);
                if (hash == null) return;
                hash.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
            });
        }

        public override void PersistSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                foreach (var set in realm.All<SetDto>().Where(_ => _.Key == key))
                {
                    set.ExpireAt = null;
                }
            });
        }

        public override void PersistList(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                foreach (var list in realm.All<ListDto>().Where(_ => _.Key == key))
                {
                    list.ExpireAt = null;
                }
            });

        }

        public override void PersistHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var hash = realm.Find<HashDto>(key);
                if (hash == null) return;
                hash.ExpireAt = null;
            });
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            foreach (var item in items)
            {
                AddToSet(key, item);
            }
            
        }

        public override void RemoveSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var query = realm.All<SetDto>().Where(_ => _.Key.StartsWith(key));
                realm.RemoveRange(query);
            });
            
        }

        public override void Dispose()
        {
            base.Dispose();
            //_transaction?.Dispose();
            //_realm?.Dispose();
        }
    }
}