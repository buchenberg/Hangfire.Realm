using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.States;
using Hangfire.Storage;
using Realms;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Realm.DAL
{
    public sealed class RealmWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly ILog _logger;
        private readonly RealmJobStorage _storage;
        private readonly Queue<Action<Realms.Realm>> _commandQueue;

        public RealmWriteOnlyTransaction(RealmJobStorage storage)
        {
            _logger = LogProvider.For<RealmWriteOnlyTransaction>();
            _storage = storage;
            _commandQueue = new Queue<Action<Realms.Realm>>();
        }
        public override void Commit()
        {
            using var realm = _storage.GetRealm();
            using var transaction = realm.BeginWrite();
            foreach (var command in _commandQueue)
            {
                command(realm);
            }
            transaction.Commit();
        }
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job == null) return;
                job.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });
        }
        public override void PersistJob(string jobId)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job == null) return;
                job.ExpireAt = null;
            });
        }
        public override void SetJobState(string jobId, IState state)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
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

                job.StateName = state.Name;
                job.StateHistory.Insert(0, stateData);
            });
        }
        public override void AddJobState(string jobId, IState state)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
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

                job.StateHistory.Insert(0, stateData);
            });
        }
        public override void AddToQueue(string queue, string jobId)
        {
            QueueCommand(r =>
            {
                r.Add(new JobQueueDto
                {
                    Queue = queue,
                    JobId = jobId
                });
            });
            RealmJobQueue.NewItemInQueueEvent.Set();
        }
        public override void IncrementCounter(string key)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.Value.Increment();
            });
        }
        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
                counter.Value.Increment();
            });
        }
        public override void DecrementCounter(string key)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.Value.Decrement();
            });
        }
        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
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
            QueueCommand(r =>
            {
                var set = r
                    .All<SetDto>()
                    .SingleOrDefault(_ => _.Key == key && _.Value == value) ?? r.Add(new SetDto() 
                {
                    Key = key,
                    Value = value
                });
                set.Score = score;
            });
        }
        public override void RemoveFromSet(string key, string value)
        {
            QueueCommand(r =>
            {
                var set = r
                    .All<SetDto>()
                    .SingleOrDefault(_ => _.Key == key && _.Value == value);
                if (set == null) return;
                r.Remove(set);
            });
        }
        public override void InsertToList(string key, string value)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key) ?? r.Add(new ListDto(key));
                list.Values.Add(value);
            });
        }
        public override void RemoveFromList(string key, string value)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key);
                if (list == null) return;

                var matchingValues = list.Values.Where(_ => _ == value).ToArray();
                if (!matchingValues.Any()) return;

                foreach (var match in matchingValues)
                {
                    list.Values.Remove(match);
                }
            });
        }
        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key);
                if (list == null) return;
                for (var i = list.Values.Count - 1; i >= 0; i--)
                {
                    if (i < keepStartingFrom || i > keepEndingAt)
                    {
                        list.Values.RemoveAt(i);
                    }
                }
            });
        }
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {

            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));
            var valuePairs = keyValuePairs as KeyValuePair<string, string>[] ?? keyValuePairs.ToArray();

            QueueCommand(r =>
            {
                var hash = r.Find<HashDto>(key) ?? r.Add(new HashDto(key));
                foreach (var pair in valuePairs)
                {
                    var matchingField = hash.Fields.SingleOrDefault(_ => _.Key == pair.Key);
                    if (matchingField != null)
                    {
                        matchingField.Value = pair.Value;
                    }
                    else
                    {
                        hash.Fields.Add(new FieldDto(pair.Key, pair.Value));
                    }
                }
            });
        }
        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            QueueCommand(r =>
            {
                var hash = r.Find<HashDto>(key);
                if (hash == null) return;
                r.Remove(hash);
            });
        }

        // New methods to support Hangfire pro feature - batches.

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var expireAt = DateTimeOffset.UtcNow.Add(expireIn);

            QueueCommand(r =>
            {
                var setDtos = r.All<SetDto>().Where(_ => _.Key == key);
                if (!setDtos.Any()) return;
                foreach (var set in setDtos)
                {
                    set.ExpireAt = expireAt;
                }
            });
        }
        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            var expireAt = DateTimeOffset.UtcNow.Add(expireIn);
            QueueCommand(r =>
            {
                var listDtos = r.All<ListDto>().Where(_ => _.Key == key);
                if (!listDtos.Any()) return;
                foreach (var list in listDtos)
                {
                    list.ExpireAt = expireAt;
                }
            });
        }
        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(r =>
            {
                var hash = r.Find<HashDto>(key);
                if (hash == null) return;
                hash.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
            });
        }
        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(r =>
            {
                var setDtos = r.All<SetDto>().Where(_ => _.Key == key);
                if (!setDtos.Any()) return;
                foreach (var set in setDtos)
                {
                    set.ExpireAt = null;
                }
            });
        }
        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(r =>
            {
                var lists = r.All<ListDto>().Where(_ => _.Key == key);
                if (!lists.Any()) return;
                foreach (var list in lists)
                {
                    list.ExpireAt = null;
                }
            });
        }
        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(r =>
            {
                var hash = r.Find<HashDto>(key);
                if (hash == null) return;
                hash.ExpireAt = null;
            });
        }
        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            foreach (var item in items)
            {
                AddToSet(key, item);
            }
        }
        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            QueueCommand(r =>
            {
                var sets = r.All<SetDto>().Where(_ => _.Key.StartsWith(key));
                if (!sets.Any()) return;
                r.RemoveRange(sets);
            });
        }
        private void QueueCommand(Action<Realms.Realm> action)
        {
            _commandQueue.Enqueue(action);
        }

    }
}