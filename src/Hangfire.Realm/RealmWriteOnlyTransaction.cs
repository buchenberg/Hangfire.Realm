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
                if (counter == null) return;

                counter.Value.Increment();
            });
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key);
                if (counter == null) return;
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
                var set = r.All<SetDto>()
                    .FirstOrDefault(s => s.Key == key && s.Value == value);
                if (set != null)
                {
                    set.Score = score;
                }
                else
                {
                    r.Add(new SetDto
                    {
                        Key = key,
                        Value = value,
                        Score = score,
                        ExpireIn = null
                    });
                }
            });
        }

        public override void RemoveFromSet(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void InsertToList(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void RemoveFromList(string key, string value)
        {
            throw new NotImplementedException();
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            throw new NotImplementedException();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            throw new NotImplementedException();
        }

        public override void RemoveHash(string key)
        {
            throw new NotImplementedException();
        }

        public override void Commit()
        {
            throw new NotImplementedException();
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