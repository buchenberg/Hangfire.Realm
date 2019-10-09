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
        private readonly RealmJobStorage _storage;
        private readonly LockDto _lockDto;
        private readonly object _lockObject = new object();
        private Timer _heartbeatTimer;

        private static readonly ILog Logger = LogProvider.For<RealmDistributedLock>();
        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks
            = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());
        
        public RealmDistributedLock(string resource, TimeSpan timeout, RealmJobStorage storage)
        {
            _resource = resource ?? throw new ArgumentNullException(nameof(resource));
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));

            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                
                _lockDto = GetLock();
                Acquire(timeout);
                AcquiredLocks.Value[_resource] = 1;
                _heartbeatTimer = StartHeartBeat();
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }
        }

        private void Acquire(TimeSpan timeout)
        {
            try
            {
                var now = DateTime.UtcNow;
                var lockTimeoutTime = now.Add(timeout);
                while (true)
                {
                    var gotLock = false;
                    var realm = _storage.GetRealm();
                    realm.Write(() =>
                    {
                        // if not in db or expired add or update
                        if (_lockDto.IsAcquired)
                        {
                            return;
                        }
                        
                        _lockDto.ExpireAt = DateTimeOffset.UtcNow.Add(_storage.Options.DistributedLockLifetime);
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
        
        private LockDto GetLock()
        {
            LockDto lockDto = null;
            var realm = _storage.GetRealm();
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
        
        private Timer StartHeartBeat()
        {
            TimeSpan distributedLockLifetime = _storage.Options.DistributedLockLifetime;
            var timerInterval = TimeSpan.FromMilliseconds(distributedLockLifetime.TotalMilliseconds / 5);
            return new Timer(state =>
            {
                // Timer callback may be invoked after the Dispose method call,
                // so we are using lock to avoid un synchronized calls.
                lock (_lockObject)
                {
                    try
                    {

                        var realm = _storage.GetRealm();
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
                var realm = _storage.GetRealm();
                realm.Write(() => { _lockDto.ExpireAt = null; });
                
                if (Logger.IsTraceEnabled())
                {
                    Logger.Trace($"{_resource} - Release");    
                }
                
            }

            
        }
    }
}