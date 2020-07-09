using System;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Realm.DAL;
using Hangfire.Realm.Models;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    [Obsolete]
    internal class ExpirationManager : IServerComponent
    {
        private readonly ILog _logger = LogProvider.For<ExpirationManager>();
        private readonly RealmJobStorage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(RealmJobStorage storage, TimeSpan checkInterval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (var realm = _storage.GetRealm())
            using (var transaction = realm.BeginWrite())
            {
                _logger.Debug("Removing outdated records...");

                var expiredJobRecords = realm.All<JobDto>().Where(_ => _.ExpireAt < DateTimeOffset.UtcNow);
                _logger.Debug($"Removing {expiredJobRecords.Count()} outdated job records...");
                
                foreach (var job in expiredJobRecords)
                {
                    foreach (var param in job.Parameters)
                    {
                        realm.Remove(param);
                    }
                    foreach (var state in job.StateHistory)
                    {
                        foreach (var data in state.Data)
                        {
                            realm.Remove(data);
                        }
                        realm.Remove(state);
                    }

                    realm.Remove(job);
                }


                var expiredListRecords = realm.All<ListDto>().Where(_ => _.ExpireAt < DateTimeOffset.UtcNow);
                _logger.Debug($"Removing {expiredListRecords.Count()} outdated list records...");
                realm.RemoveRange(expiredListRecords);

                var expiredSetRecords = realm.All<SetDto>().Where(_ => _.ExpireAt < DateTimeOffset.UtcNow);
                _logger.Debug($"Removing {expiredSetRecords.Count()} outdated set records...");
                realm.RemoveRange(expiredSetRecords);

                var expiredHashRecords = realm.All<HashDto>().Where(_ => _.ExpireAt < DateTimeOffset.UtcNow);
                _logger.Debug($"Removing {expiredHashRecords.Count()} outdated hash records...");
                realm.RemoveRange(expiredHashRecords);
                transaction.Commit();
            }
            
            cancellationToken.Wait(_checkInterval);
        }


    }
    }
