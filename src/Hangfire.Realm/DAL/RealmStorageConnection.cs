using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Hangfire.Realm.DAL
{
    internal sealed class RealmStorageConnection : JobStorageConnection
    {
        private readonly RealmJobStorage _storage;
        private readonly RealmJobQueue _jobQueue;
        private readonly ILog _logger;
        private readonly object _lockObject;

        public RealmStorageConnection(
            RealmJobStorage storage)
        {
            _logger = LogProvider.For<RealmStorageConnection>();
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _jobQueue = new RealmJobQueue(storage);
            _lockObject = new object();
        }
        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            lock (_lockObject)
            {
                return new RealmWriteOnlyTransaction(_storage);
            }

        }
        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => null; //No locks in RealmDb

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));

            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            using var realm = _storage.GetRealm();
            using var transaction = realm.BeginWrite();
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
            transaction.Commit();

            return jobDto.Id;

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
                using var realm = _storage.GetRealm();
                using var transaction = realm.BeginWrite();
                var jobDto = realm.Find<JobDto>(id);
                var jobParams = jobDto.Parameters
                    .Where(_ => _.Key == name)
                    .ToList();
                if (jobParams.Any())
                {
                    jobParams.Single().Value = value;
                }
                else
                {
                    jobDto.Parameters.Add(new ParameterDto(name, value));
                }
                realm.Add(jobDto, update: true);
                transaction.Commit();
            }
            catch (Exception e)
            {
                _logger.ErrorException($"Error setting job parameter.", e);
                throw;
            }
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));
            using var realm = _storage.GetRealm();
            var jobDto = realm.Find<JobDto>(id);
            var param = jobDto.Parameters.FirstOrDefault(_ => _.Key == name);
            return param?.Value;
        }

        [CanBeNull]
        public override JobData GetJobData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            using var realm = _storage.GetRealm();
            var jobDto = realm.Find<JobDto>(jobId);

            if (jobDto == null) return null;

            var invocationData = SerializationHelper.Deserialize<InvocationData>(jobDto.InvocationData);
            invocationData.Arguments = jobDto.Arguments;

            Job job = null;
            JobLoadException loadException = null;

            try
            {
                job = invocationData.DeserializeJob();
            }
            catch (JobLoadException ex)
            {
                _logger.WarnException($"Deserializing job {jobId} has failed.", ex);
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = jobDto.StateName,
                CreatedAt = jobDto.Created.DateTime,
                LoadException = loadException
            };
        }

        [CanBeNull]
        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            try
            {
                using var realm = _storage.GetRealm();
                var state = realm.Find<JobDto>(jobId).StateHistory.OrderByDescending(_ => _.Created)
                    .FirstOrDefault();
                if (state == null) return null;
                return new StateData
                {
                    Name = state.Name,
                    Reason = state.Reason,
                    Data = state.Data.ToDictionary(x => x.Key, x => x.Value)
                };
            }
            catch (Exception e)
            {
                _logger.ErrorException($"Error getting state data for job {jobId}.", e);
                throw;
            }
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
            try
            {
                using var realm = _storage.GetRealm();
                using var transaction = realm.BeginWrite();
                var server = new ServerDto
                {
                    Id = serverId,
                    WorkerCount = context.WorkerCount,
                    StartedAt = DateTime.UtcNow,
                    LastHeartbeat = DateTime.UtcNow
                };
                ((List<string>)server.Queues).AddRange(context.Queues);
                realm.Add(server, update: true);
                transaction.Commit();
            }
            catch (Exception e)
            {
                _logger.ErrorException($"Error announcing {serverId}.", e);
                throw;
            }

        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            try
            {
                using var realm = _storage.GetRealm();
                using var transaction = realm.BeginWrite();
                var server = realm.Find<ServerDto>(serverId);
                realm.Remove(server);
                transaction.Commit();
            }
            catch (Exception e)
            {
                _logger.ErrorException($"Error removing server {serverId}.", e);
                throw;
            }
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            try
            {
                using var realm = _storage.GetRealm();
                using var transaction = realm.BeginWrite();
                var servers = realm.All<ServerDto>()
                    .Where(d => d.Id == serverId);
                foreach (var server in servers)
                {
                    server.LastHeartbeat = DateTime.UtcNow;
                }
                transaction.Commit();
            }
            catch (Exception e)
            {
                _logger.ErrorException($"Error sending heartbeat for server {serverId}.", e);
                throw;
            }
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            try
            {
                var cutoff = DateTime.UtcNow.Add(timeOut.Negate());
                var deletedServerCount = 0;
                using var realm = _storage.GetRealm();
                using var transaction = realm.BeginWrite();
                var servers = realm.All<ServerDto>()
                    .Where(_ => _.LastHeartbeat < cutoff);
                foreach (var server in servers)
                {
                    realm.Remove(server);
                    deletedServerCount++;
                }
                transaction.Commit();
                return deletedServerCount;
            }
            catch (Exception e)
            {
                _logger.ErrorException($"Error removing timed out servers.", e);
                throw;
            }
        }
        public override List<string> GetRangeFromSet([NotNull] string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
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
            using var realm = _storage.GetRealm();
            var result = realm.All<SetDto>()
                .Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.Value);
            return new HashSet<string>(result);
        }
        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
            return realm.All<SetDto>()
                .Count(_ => _.Key == key);
        }
        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
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
            using var realm = _storage.GetRealm();
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
            var valuePairs = keyValuePairs as KeyValuePair<string, string>[] ?? keyValuePairs.ToArray();
            using var realm = _storage.GetRealm();
            using var transaction = realm.BeginWrite();
            var hash = realm.Find<HashDto>(key) ?? realm.Add(new HashDto(key));
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
            transaction.Commit();
        }
        [CanBeNull]
        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
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
            using var realm = _storage.GetRealm();
            var result = realm.All<SetDto>().Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.ExpireAt)
                .Min();
            return result.HasValue ? result.Value - DateTimeOffset.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
            return realm.Find<CounterDto>(key).Value;
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
            var result = realm.All<HashDto>().Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.ExpireAt)
                .Min();
            return result.HasValue ? result.Value - DateTimeOffset.UtcNow : TimeSpan.FromSeconds(-1);
        }
        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
            var result = realm
                .All<HashDto>()
                .Count(_ => _.Key == key);
            return (long)result;
        }
        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));
            using var realm = _storage.GetRealm();
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
            using var realm = _storage.GetRealm();
            var result = realm
                .All<ListDto>()
                .Count(_ => _.Key == key);
            return (long)result;
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
            var result = realm.All<ListDto>().Where(_ => _.Key == key)
                .ToList()
                .Select(_ => _.ExpireAt)
                .Min();
            return result.HasValue ? result.Value - DateTimeOffset.UtcNow : TimeSpan.FromSeconds(-1);
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            using var realm = _storage.GetRealm();
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
            using var realm = _storage.GetRealm();
            return realm.All<ListDto>()
                .Where(_ => _.Key == key)
                .OrderByDescending(_ => _.Created)
                .ToList()
                .SelectMany(_ => _.Values)
                .ToList();
        }

    }
}
