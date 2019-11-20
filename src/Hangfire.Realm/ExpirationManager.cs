using System;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    internal class ExpirationManager : IServerComponent
    {
        private const string DistributedLockKey = "locks:expirationmanager";
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);
        private readonly ILog _logger = LogProvider.For<ExpirationManager>();
        private readonly RealmJobStorage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(RealmJobStorage storage, TimeSpan checkInterval)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            _storage = storage;
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (var distLock = new RealmDistributedLock(DistributedLockKey, DefaultLockTimeout, _storage))
            {
                var realm = _storage.GetRealm();
                realm.Write(() =>
                {
                    _logger.Debug("Removing outdated records...");

                    var expiredJobRecords = realm.All<JobDto>().Where(_ => _.ExpireAt > DateTimeOffset.UtcNow);
                    _logger.Debug($"Removing {expiredJobRecords.Count()} outdated job records...");
                    realm.RemoveRange(expiredJobRecords);

                    var expiredListRecords = realm.All<ListDto>().Where(_ => _.ExpireAt > DateTimeOffset.UtcNow);
                    _logger.Debug($"Removing {expiredListRecords.Count()} outdated list records...");
                    realm.RemoveRange(expiredListRecords);

                    var expiredSetRecords = realm.All<SetDto>().Where(_ => _.ExpireAt > DateTimeOffset.UtcNow);
                    _logger.Debug($"Removing {expiredSetRecords.Count()} outdated set records...");
                    realm.RemoveRange(expiredSetRecords);

                    var expiredHashRecords = realm.All<HashDto>().Where(_ => _.ExpireAt > DateTimeOffset.UtcNow);
                    _logger.Debug($"Removing {expiredHashRecords.Count()} outdated hash records...");
                    realm.RemoveRange(expiredHashRecords);

                });
                
            }
            cancellationToken.Wait(_checkInterval);
        }

        }
    }
