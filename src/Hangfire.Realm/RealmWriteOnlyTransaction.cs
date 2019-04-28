using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Realm.Models;
using Hangfire.States;
using Hangfire.Storage;
using Realms;

namespace Hangfire.Realm
{
    public class RealmWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Realms.Realm _realm;
        private readonly Transaction _transaction;
        
        public RealmWriteOnlyTransaction(IRealmDbContext realmDbContext)
        {
            _realm = realmDbContext.GetRealm();
            _transaction =  _realm.BeginWrite();
        }
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            var job = _realm.Find<JobDto>(jobId);
            if (job != null)
            {
                job.ExpireAt = DateTime.UtcNow.Add(expireIn);
            }
        }

        public override void PersistJob(string jobId)
        {
            var job = _realm.Find<JobDto>(jobId);
            if (job != null)
            {
                job.ExpireAt = null;
            }
        }

        public override void SetJobState(string jobId, IState state)
        {
            var job = _realm.Find<JobDto>(jobId);
            if (job == null) return;
                
            job.StateName = state.Name;
            InsertStateHistory(job, state);
        }

        public override void AddJobState(string jobId, IState state)
        {
            var job = _realm.Find<JobDto>(jobId);
            if (job == null) return;
                
            InsertStateHistory(job, state);
        }
        
        private static void InsertStateHistory(JobDto jobDto, IState state)
        {
            var stateData = new StateDto
            {
                Reason = state.Reason,
                Name = state.Name
            };
            
            foreach (var data in state.SerializeData())
            {
                stateData.Data.Add(new StateDataDto(data));
            }
                
            jobDto.StateHistory.Insert(0, stateData);
        }

        public override void AddToQueue(string queue, string jobId)
        {
            _realm.Add(new QueuedJobDto {Created = DateTimeOffset.UtcNow, Queue = queue, JobId = jobId});
        }

        public override void IncrementCounter(string key)
        {
            var counter = _realm.Find<CounterDto>(key);
            if (counter == null)
            {
                counter = _realm.Add(new CounterDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = key, 
                    Value = 0
                });
            }

            counter.Value.Increment();
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            var counter = _realm.Find<CounterDto>(key);
            if (counter == null)
            {
                counter = _realm.Add(new CounterDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = key, 
                    Value = 0
                });
            }

                
            counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);   
            counter.Value.Increment();
        }

        public override void DecrementCounter(string key)
        {
            var counter = _realm.Find<CounterDto>(key);
            if (counter == null) 
            {
                counter = _realm.Add(new CounterDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = key, 
                    Value = 0
                });
            }

            counter.Value.Decrement();
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            var counter = _realm.Find<CounterDto>(key);
            if (counter == null)
            {
                counter = _realm.Add(new CounterDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = key,
                    Value = 0
                });
            }

            counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);   
            counter.Value.Decrement();
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            var compoundKey = SetDto.CreateCompoundKey(key, value);
            var set = _realm.Find<SetDto>(compoundKey);

            if (set == null)
            {
                _realm.Add(new SetDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = compoundKey,
                    ExpireAt = null,
                    Value = value,
                    Score = score
                });
                return;
            }

            set.Score = score;
        }

        public override void RemoveFromSet(string key, string value)
        {
            var compoundKey = SetDto.CreateCompoundKey(key, value);
            var set = _realm.Find<SetDto>(compoundKey);
                
            if (set == null) return;

            _realm.Remove(set);
        }

        public override void InsertToList(string key, string value)
        {
            var list = _realm.Find<ListDto>(key);
            if (list == null)
            {
                list = _realm.Add(new ListDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = key,
                    ExpireAt = null
                });
            }

            list.Values.Add(value);
        }

        public override void RemoveFromList(string key, string value)
        {
            var list = _realm.Find<ListDto>(key);
            if (list == null)
            {
                return;
            }
            for (int i = list.Values.Count -1; i >= 0; i--)
            {
                var listValue = list.Values[i];
                if (listValue == value)
                {
                    list.Values.RemoveAt(i);
                }
            }
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var list = _realm.Find<ListDto>(key);
            if (list == null)
            {
                return;
            }

            //delete from cte where row_num not between @start and @end";
            for (int i = list.Values.Count -1; i >= 0; i--)
            {
                if (i < keepStartingFrom || i > keepEndingAt)
                {
                    list.Values.RemoveAt(i);        
                }
            }
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (keyValuePairs == null)
            {
                throw new ArgumentNullException(nameof(keyValuePairs));
            }
            
            var hash = _realm.Find<HashDto>(key);
            if (hash == null)
            {
                hash = _realm.Add(new HashDto
                {
                    Created = DateTimeOffset.UtcNow,
                    Key = key
                });
            }
                
            foreach (var valuePair in keyValuePairs)
            {
                var field = hash.Fields.FirstOrDefault(f => f.Key == valuePair.Key);
                if (field == null)
                {
                    field = new FieldDto
                    {
                        Key = valuePair.Key,
                        Value = valuePair.Value
                    };
                    hash.Fields.Add(field);
                    continue;
                }

                field.Value = valuePair.Value;
            }
        }

        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            var hash = _realm.Find<HashDto>(key);
            if(hash == null) return;
                
            _realm.Remove(hash);
        }

        public override void Commit()
        {
            _transaction.Commit();
        }
        
        // New methods to support Hangfire pro feature - batches.


        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var expireAt = DateTimeOffset.UtcNow.Add(expireIn);
            foreach (var set in _realm.All<SetDto>().Where(s => s.Key.StartsWith(key)))
            {
                set.ExpireAt = expireAt;
            }
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            var list = _realm.Find<ListDto>(key);
            if (list == null) return;
            
            list.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var hash = _realm.Find<HashDto>(key);
            if (hash == null) return;
            
            hash.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
        }


        public override void PersistSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            foreach (var set in _realm.All<SetDto>().Where(s => s.Key.StartsWith(key)))
            {
                set.ExpireAt = null;
            }
        }

        public override void PersistList(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var list = _realm.Find<ListDto>(key);
            if (list == null) return;

            list.ExpireAt = null;

        }

        public override void PersistHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            var hash = _realm.Find<HashDto>(key);
            if (hash == null) return;

            hash.ExpireAt = null;

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

            var query = _realm.All<SetDto>().Where(s => s.Key.StartsWith(key));
            _realm.RemoveRange(query);
        }

        public override void Dispose()
        {
            base.Dispose();
            _transaction.Dispose();
        }
    }
}