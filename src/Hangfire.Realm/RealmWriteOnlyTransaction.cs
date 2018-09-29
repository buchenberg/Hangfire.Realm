using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Realm.Dtos;
using Hangfire.Realm.Extensions;
using Hangfire.States;
using Hangfire.Storage;
using Realms;

namespace Hangfire.Realm
{
    public class RealmWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Realms.Realm _realm;
        private readonly Queue<Action<Realms.Realm>> _commandQueue = new Queue<Action<Realms.Realm>>();
        public RealmWriteOnlyTransaction(Realms.Realm realm)
        {
            _realm = realm;
        }
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job != null)
                {
                    job.ExpireAt = DateTime.UtcNow.Add(expireIn);
                }
            });
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job != null)
                {
                    job.ExpireAt = null;
                }
            });
        }

        public override void SetJobState(string jobId, IState state)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job == null) return;
                
                job.StateName = state.Name;
                job.AddToStateHistory(state);
            });
        }

        public override void AddJobState(string jobId, IState state)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job == null) return;
                
                job.AddToStateHistory(state);
            });
        }

        public override void AddToQueue(string queue, string jobId)
        {
            QueueCommand(r =>
            {
                r.Add(new JobQueueDto {Queue = queue, JobId = jobId});
            });
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key);
                if (counter == null)
                {
                    counter = r.Add(new CounterDto {Key = key, Value = 0});
                };

                counter.Value.Increment();
            });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key);
                if (counter == null)
                {
                    counter = r.Add(new CounterDto {Key = key, Value = 0});
                };
                counter.ExpireIn = DateTimeOffset.UtcNow.Add(expireIn);   
                counter.Value.Increment();
            });
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key);
                if (counter == null) return;

                counter.Value.Decrement();
            });
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key);
                if (counter == null) return;
                counter.ExpireIn = DateTimeOffset.UtcNow.Add(expireIn);   
                counter.Value.Decrement();
            });
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            QueueCommand(r =>
            {
                var set = r.Find<SetDto>(key);

                if (set == null)
                {
                    set = r.Add(new SetDto
                    {
                        Key = key,
                        ExpireIn = null
                    });
                }
                
                if (!set.Scores.Any(s => s.Value == value && s.Score == score))
                {
                    set.Scores.Add(new ScoreDto
                    {
                        Score = score,
                        Value = value
                    });
                }
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand(r =>
            {
                var set = r.Find<SetDto>(key);
                
                if (set == null) return;

                var score = set.Scores.FirstOrDefault(s => s.Value == value);
                if (score != null)
                {
                    set.Scores.Remove(score);
                }

                if (set.Scores.Count == 0)
                {
                    r.Remove(set);
                }
            });
        }

        public override void InsertToList(string key, string value)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key);
                if (list == null)
                {
                    list = r.Add(new ListDto
                    {
                        Key = key,
                        ExpireAt = null
                    });
                }

                if (!list.Values.Contains(value))
                {
                    list.Values.Add(value);
                }
            });
        }

        public override void RemoveFromList(string key, string value)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key);
                list.Values.Remove(value);
            });
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key);
                if (list == null)
                {
                    return;
                }

                for (var i = keepStartingFrom; i < keepEndingAt; i++)
                {
                    list.Values.RemoveAt(i);    
                }
                
            });
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            QueueCommand(r =>
            {
                var hash = r.Find<HashDto>(key);
                if (hash == null)
                {
                    hash = r.Add(new HashDto
                    {
                        Key = key
                    });
                }
                
                foreach (var valuePair in keyValuePairs)
                {
                    var field = hash.Fields.FirstOrDefault(f => f.Key == valuePair.Key);
                    if (field == null)
                    {
                        field = new KeyValueDto
                        {
                            Key = valuePair.Key,
                            Value = valuePair.Value
                        };
                        hash.Fields.Add(field);
                        continue;
                    }

                    field.Value = valuePair.Value;
                }
            });
        }

        public override void RemoveHash(string key)
        {
            QueueCommand(r =>
            {
                var hash = r.Find<HashDto>(key);
                if(hash == null) return;
                
                r.Remove(hash);
            });
        }

        public override void Commit()
        {
            _realm.Write(() =>
            {
                foreach (var command in _commandQueue)
                {
                    command(_realm);
                }
            });
        }

        private void QueueCommand(Action<Realms.Realm> action)
        {
            _commandQueue.Enqueue(action);
        }

        public override void Dispose()
        {
            base.Dispose();
        }
    }
}