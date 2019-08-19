using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Realm.Models;
using Hangfire.Storage;

namespace Hangfire.Realm
{
    public sealed class RealmDistributedLock : IDisposable
    {
        private readonly string _resource;
        private readonly IRealmDbContext _realmDbContext;
        private readonly RealmJobStorageOptions _storageOptions;
        private readonly LockDto _lockDto;
        private readonly object _lockObject = new object();
        private Timer _heartbeatTimer;

        private static readonly ILog Logger = LogProvider.For<RealmDistributedLock>();
        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
            = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());
        
        public RealmDistributedLock(string resource, TimeSpan timeout, IRealmDbContext realmDbContext, RealmJobStorageOptions storageOptions)
        {
            _resource = resource;
            _realmDbContext = realmDbContext;
            _storageOptions = storageOptions;

            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                var realm = realmDbContext.GetRealm();
                _lockDto = GetLock(realm);
                
                Acquire(realm, timeout);
                AcquiredLocks.Value[_resource] = 1;
                _heartbeatTimer = StartHeartBeat(_storageOptions.DistributedLockLifetime);
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }
        }

        private void Acquire(Realms.Realm realm, TimeSpan timeout)
        {
            try
            {
                var now = DateTime.UtcNow;
                var lockTimeoutTime = now.Add(timeout);
                while (true)
                {
                    var gotLock = false;
                    realm.Write(() =>
                    {
                        // if not in db or expired add or update
                        if (_lockDto.IsAcquired)
                        {
                            return;
                        }
                        
                        _lockDto.ExpireAt = DateTimeOffset.UtcNow.Add(_storageOptions.DistributedLockLifetime);
                        gotLock = true;
                    });
                    
                    if (gotLock)
                    {
                        if (Logger.IsTraceEnabled())
                        {
                            Logger.Trace($"{_resource} - Acquired");    
                        }
                        return;
                    }

                    now = Wait(CalculateTimeout(timeout));
                    if ((lockTimeoutTime < now))
                    {
                        throw new DistributedLockTimeoutException(_resource);
                    }
                }
            }
            catch (DistributedLockTimeoutException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new RealmDistributedLockException($"{_resource} - Could not place a lock", e);
            }
        }

        private static DateTime Wait(TimeSpan timeout)
        {
            Thread.Sleep(timeout);
            return DateTime.UtcNow;
        }
        
        private LockDto GetLock(Realms.Realm realm)
        {
            LockDto lockDto = null;
            realm.Write(() =>
            {
                lockDto = realm.Find<LockDto>(_resource);
                if (lockDto == null || lockDto?.ExpireAt != null && lockDto.ExpireAt < DateTimeOffset.UtcNow)
                {
                    lockDto = realm.Add(new LockDto
                    {
                        Resource = _resource,
                        ExpireAt = null
                    }, update: true);
                }
            });
            return lockDto;
        }
        
        private static TimeSpan CalculateTimeout(TimeSpan timeout)
        {
            return TimeSpan.FromMilliseconds((timeout.TotalMilliseconds / 1000) + 5);
        }
        
        private Timer StartHeartBeat(TimeSpan distributedLockLifetime)
        {
            var timerInterval = TimeSpan.FromMilliseconds(distributedLockLifetime.TotalMilliseconds / 5);
            return new Timer(state =>
            {
                // Timer callback may be invoked after the Dispose method call,
                // so we are using lock to avoid un synchronized calls.
                lock (_lockObject)
                {
                    try
                    {

                        var realm = _realmDbContext.GetRealm();
                        realm.Write(() =>
                        {
                            _lockDto.ExpireAt = DateTimeOffset.UtcNow.Add(distributedLockLifetime);
                        });
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"{_resource} - Unable to update heartbeat on the resource. Details:\r\n{ex}");
                    }
                }
            }, null, timerInterval, timerInterval);
        }
        
        public void Dispose()
        {
            if (!AcquiredLocks.Value.ContainsKey(_resource))
            {
                return;
            }

            AcquiredLocks.Value[_resource]--;

            if (AcquiredLocks.Value[_resource] > 0)
            {
                return;
            }

            lock (_lockObject)
            {
                AcquiredLocks.Value.Remove(_resource);

                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }
                var realm = _realmDbContext.GetRealm();
                realm.Write(() => { _lockDto.ExpireAt = null; });

                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"{_resource} - Release");    
                }
                
            }

            
        }
    }
}