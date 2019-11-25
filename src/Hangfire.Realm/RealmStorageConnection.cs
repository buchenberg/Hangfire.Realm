﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    internal class RealmStorageConnection : JobStorageConnection
    {
        private readonly RealmJobStorage _storage;
        private readonly RealmJobQueue _jobQueue;
        private static readonly ILog Logger = LogProvider.For<RealmStorageConnection>();

        public RealmStorageConnection(
            RealmJobStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _jobQueue = new RealmJobQueue(storage);
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RealmWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {

            return new RealmDistributedLock(resource, timeout, _storage);
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            string jobId;

            using (var transaction = new RealmWriteOnlyTransaction(_storage))
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
            try
            {
                var realm = _storage.GetRealm();
                realm.Write(() =>
                {
                    var jobDto = realm.Find<JobDto>(id);
                    var jobParams = jobDto.Parameters.Where(_ => _.Key == name);
                    if (jobParams.Any())
                    {
                        jobParams.Single().Value = value;
                    }
                    else
                    {
                        jobDto.Parameters.Add(new ParameterDto(name, value));
                    }
                    realm.Add(jobDto, update: true);
                });
            }
            catch (Exception e)
            {
                Logger.ErrorException($"Error setting job parameter.", e);
                throw;
            }
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));
            var realm = _storage.GetRealm();
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
            var realm = _storage.GetRealm();
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
                Logger.WarnException($"Deserializing job {jobId} has failed.", ex);
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
            try
            {
                if (jobId == null)
                {
                    throw new ArgumentNullException(nameof(jobId));
                }

                var realm = _storage.GetRealm();
                var state = realm.Find<JobDto>(jobId).StateHistory.OrderByDescending(_ => _.Created).FirstOrDefault();
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
            catch (Exception e)
            {
                Logger.ErrorException($"Error getting state data for job {jobId}.", e);
                throw;
            }
        }

        public override void AnnounceServer(string serverId, ServerContext context)
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
                using (var transaction = new RealmWriteOnlyTransaction(_storage))
                {
                    transaction.AnnounceServer(serverId, context);
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                Logger.ErrorException($"Error announcing {serverId}.", e);
                throw;
            }

        }
        //TODO Write only (own thread)
        public override void RemoveServer(string serverId)
        {
            try
            {
                if (serverId == null) throw new ArgumentNullException(nameof(serverId));
                var realm = _storage.GetRealm();
                realm.Write(() =>
                {
                    var server = realm.Find<ServerDto>(serverId);
                    realm.Remove(server);
                });
            }
            catch (Exception e)
            {
                Logger.ErrorException($"Error removing server {serverId}.", e);
                throw;
            }
        }
        //TODO Write only (own thread)
        public override void Heartbeat(string serverId)
        {
            try
            {
                if (serverId == null) throw new ArgumentNullException(nameof(serverId));
                var realm = _storage.GetRealm();
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
            catch (Exception e)
            {
                Logger.ErrorException($"Error sending heartbeat for server {serverId}.", e);
                throw;
            }
        }
        //TODO Write only (own thread)
        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            try
            {
                if (timeOut.Duration() != timeOut)
                    throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
                DateTime cutoff = DateTime.UtcNow.Add(timeOut.Negate());
                int deletedServerCount = 0;
                var realm = _storage.GetRealm();
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
                return deletedServerCount;
            }
            catch (Exception e)
            {
                Logger.ErrorException($"Error removing timed out servers.", e);
                throw;
            }
        }
        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            return realm.All<SetDto>()
                .Where(_ => _.Key == key)
                .OrderByDescending(_ => _.Created)
                .ToList()
                .Select(_ => _.Value)
                .ToList();
        }
        [NotNull]
        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<SetDto>()
            .Where(_ => _.Key == key)
            .ToList()
            .Select(_ => _.Value);
            return new HashSet<string>(result);
        }
        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            return realm.All<SetDto>().Where(_ => _.Key == key).Count();
        }
        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");
            return GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

        }
        public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
        {
            if (count <= 0)
                throw new ArgumentException("The value must be a positive number", nameof(count));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.", nameof(toScore));
            var realm = _storage.GetRealm();
            return realm.All<SetDto>()
            .Where(_ => _.Key == key)
            .Where(_ => _.Score >= fromScore && _.Score <= toScore)
            .OrderBy(_ => _.Score)
            .ToList()
            .Take(count)
            .Select(_ => _.Value)
            .ToList();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));
            using (var transaction = new RealmWriteOnlyTransaction(_storage))
            {
                transaction.SetRangeInHash(key, keyValuePairs);
                transaction.Commit();
            }
        }
        [CanBeNull]
        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<HashDto>()
            .Where(_ => _.Key == key)
            .ToList()
            .SelectMany(_ => _.Fields)
            .ToList()
            .GroupBy(_ => _.Key, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(_ => _.Last().Key, _ => _.Last().Value);
            return result.Count != 0 ? result : null;
        }
        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<SetDto>().Where(_ => _.Key == key)
            .ToList()
            .Select(_ => _.ExpireAt)
            .Min();
            return result.HasValue ? result.Value - DateTimeOffset.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            return realm.Find<CounterDto>(key).Value;
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<HashDto>().Where(_ => _.Key == key)
            .ToList()
            .Select(_ => _.ExpireAt)
            .Min();
            return result.HasValue ? result.Value - DateTimeOffset.UtcNow : TimeSpan.FromSeconds(-1);
        }
        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<HashDto>()
            .Where(_ => _.Key == key)
            .Count();
            return (long)result;

        }
        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));
            var realm = _storage.GetRealm();
            return realm.All<HashDto>()
            .Where(_ => _.Key == key)
            .ToList()
            .SelectMany(_ => _.Fields)
            .Where(_ => _.Key == name)
            .Select(_ => _.Value)
            .First();
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<ListDto>()
            .Where(_ => _.Key == key)
            .Count();
            return (long)result;
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            var result = realm.All<ListDto>().Where(_ => _.Key == key)
            .ToList()
            .Select(_ => _.ExpireAt)
            .Min();
            return result.HasValue ? result.Value - DateTimeOffset.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            return realm.All<ListDto>()
                .Where(_ => _.Key == key)
                .OrderByDescending(_ => _.Created)
                .ToList()
                .SelectMany(_ => _.Values)
                .ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            var realm = _storage.GetRealm();
            return realm.All<ListDto>()
                .Where(_ => _.Key == key)
                .OrderByDescending(_ => _.Created)
                .ToList()
                .SelectMany(_ => _.Values)
                .ToList();
        }

    }
}
