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
            //try
            //{
            //    _transaction.Commit();
            //}
            //catch (Exception e)
            //{
            //    _logger.Error($"Error occured committing write transaction. {e.Message}");
            //    _transaction.Rollback();
            //    throw;
            //}
        }
        public override void Dispose()
        {
            base.Dispose();
        }
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job == null) return;
                job.ExpireAt = DateTime.UtcNow.Add(expireIn);
            });

            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var job = realm.Find<JobDto>(jobId);
            //if (job == null) return;
            //job.ExpireAt = DateTime.UtcNow.Add(expireIn);
            //transaction.Commit();
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(r =>
            {
                var job = r.Find<JobDto>(jobId);
                if (job == null) return;
                job.ExpireAt = null;
            });
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var job = realm.Find<JobDto>(jobId);
            //if (job == null) return;
            //job.ExpireAt = null;
            //transaction.Commit();
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

            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var job = realm.Find<JobDto>(jobId);
            //if (job == null) return;
            //var stateData = new StateDto
            //{
            //    Reason = state.Reason,
            //    Name = state.Name
            //};
            //foreach (var data in state.SerializeData())
            //{
            //    stateData.Data.Add(new StateDataDto(data));
            //}

            //job.StateName = state.Name;
            //job.StateHistory.Insert(0, stateData);
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();

            //var job = realm.Find<JobDto>(jobId);
            //if (job == null) return;
            //var stateData = new StateDto
            //{
            //    Reason = state.Reason,
            //    Name = state.Name
            //};

            //foreach (var data in state.SerializeData())
            //{
            //    stateData.Data.Add(new StateDataDto(data));
            //}

            //job.StateHistory.Insert(0, stateData);

            //transaction.Commit();

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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //realm.Add(new JobQueueDto
            //{
            //    Queue = queue,
            //    JobId = jobId
            //});
            //transaction.Commit();
            RealmJobQueue.NewItemInQueueEvent.Set();
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.Value.Increment();
            });
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var counter = realm.Find<CounterDto>(key) ?? realm.Add(new CounterDto(key));
            //counter.Value.Increment();
            //transaction.Commit();
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
                counter.Value.Increment();
            });
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var counter = realm.Find<CounterDto>(key) ?? realm.Add(new CounterDto(key));
            //counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
            //counter.Value.Increment();
            //transaction.Commit();
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.Value.Decrement();
            });
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var counter = realm.Find<CounterDto>(key) ?? realm.Add(new CounterDto(key));
            //counter.Value.Decrement();
            //transaction.Commit();
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(r =>
            {
                var counter = r.Find<CounterDto>(key) ?? r.Add(new CounterDto(key));
                counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
                counter.Value.Decrement();
            });
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var counter = realm.Find<CounterDto>(key) ?? realm.Add(new CounterDto(key));
            //counter.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
            //counter.Value.Decrement();
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var set = realm.All<SetDto>()
            //              .SingleOrDefault(_ => _.Key == key && _.Value == value) ?? realm.Add(new SetDto()
            //              {
            //                  Key = key,
            //                  Value = value
            //              });
            //set.Score = score;
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var set = realm
            //    .All<SetDto>()
            //    .SingleOrDefault(_ => _.Key == key && _.Value == value);
            //if (set == null) return;
            //realm.Remove(set);
            //transaction.Commit();
        }
        public override void InsertToList(string key, string value)
        {
            QueueCommand(r =>
            {
                var list = r.Find<ListDto>(key) ?? r.Add(new ListDto(key));
                list.Values.Add(value);
            });
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var list = realm.Find<ListDto>(key) ?? realm.Add(new ListDto(key));
            //list.Values.Add(value);
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var list = realm.Find<ListDto>(key);
            //if (list == null) return;

            //var matchingValues = list.Values.Where(_ => _ == value).ToArray();
            //if (!matchingValues.Any()) return;

            //foreach (var match in matchingValues)
            //{
            //    list.Values.Remove(match);
            //}
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var list = realm.Find<ListDto>(key);
            //if (list == null) return;
            //for (var i = list.Values.Count - 1; i >= 0; i--)
            //{
            //    if (i < keepStartingFrom || i > keepEndingAt)
            //    {
            //        list.Values.RemoveAt(i);
            //    }
            //}
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var hash = realm.Find<HashDto>(key) ?? realm.Add(new HashDto(key));
            //foreach (var pair in valuePairs)
            //{
            //    var matchingField = hash.Fields.SingleOrDefault(_ => _.Key == pair.Key);
            //    if (matchingField != null)
            //    {
            //        matchingField.Value = pair.Value;
            //    }
            //    else
            //    {
            //        hash.Fields.Add(new FieldDto(pair.Key, pair.Value));
            //    }
            //}
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var hash = realm.Find<HashDto>(key);
            //if (hash == null) return; 
            //realm.Remove(hash);
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var setDtos = realm.All<SetDto>().Where(_ => _.Key == key);
            //if (!setDtos.Any()) return;
            //foreach (var set in setDtos)
            //{
            //    set.ExpireAt = expireAt;
            //}
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var listDtos = realm.All<ListDto>().Where(_ => _.Key == key);
            //if (!listDtos.Any()) return;
            //foreach (var list in listDtos)
            //{
            //    list.ExpireAt = expireAt;
            //}
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var hash = realm.Find<HashDto>(key);
            //if (hash == null) return;
            //hash.ExpireAt = DateTimeOffset.UtcNow.Add(expireIn);
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var setDtos = realm.All<SetDto>().Where(_ => _.Key == key);
            //if (!setDtos.Any()) return;
            //foreach (var set in setDtos)
            //{
            //    set.ExpireAt = null;
            //}
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var lists = realm.All<ListDto>().Where(_ => _.Key == key);
            //if (!lists.Any()) return;
            //foreach (var list in lists)
            //{
            //    list.ExpireAt = null;
            //}
            //transaction.Commit();

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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var hash = realm.Find<HashDto>(key);
            //if (hash == null) return;
            //hash.ExpireAt = null;
            //transaction.Commit();
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
            //using var realm = _storage.GetRealm();
            //using var transaction = realm.BeginWrite();
            //var sets = realm.All<SetDto>().Where(_ => _.Key.StartsWith(key));
            //if (!sets.Any()) return;
            //realm.RemoveRange(sets);
            //transaction.Commit();
        }

        private void QueueCommand(Action<Realms.Realm> action)
        {
            _commandQueue.Enqueue(action);
        }




    }
}