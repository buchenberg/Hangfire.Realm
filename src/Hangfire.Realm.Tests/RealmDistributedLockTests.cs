using System;
using System.Linq;
using System.Threading;
using Hangfire.Realm.Models;
using Hangfire.Realm.Tests.Utils;
using Hangfire.Storage;
using NUnit.Framework;

namespace Hangfire.Realm.Tests
{
    [TestFixture]
    public class RealmDistributedLockTests
    {
        private RealmJobStorage _storage;
        
        [SetUp]
        public void Init()
        {
            _storage = new RealmJobStorage(new RealmJobStorageOptions()
            {
                RealmConfiguration = ConnectionUtils.GetRealmConfiguration()
            });
            var realm = _storage.GetRealm();
            realm.Write(() => realm.RemoveAll());
        }

        [Test]
        public void Acquire_ResourceIsNotLocked_LockAcquired()
        {
            // ARRANGE
            bool lockAcquired;
            var realm = _storage.GetRealm();

            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
            {
                lockAcquired = realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
            }
 
            // ASSERT
            Assert.True(lockAcquired);
        }

        [Test]
        public void Acquire_WithinSameThread_LockAcquired()
        {
            // ARRANGE
            bool lockAcquired1;
            bool lockAcquired2;
            var realm = _storage.GetRealm();
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
            {
                lockAcquired1 = realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
                using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
                {
                    lockAcquired2 = realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
                }
            }
 
            // ASSERT
            Assert.True(lockAcquired1);
            Assert.True(lockAcquired2);
        }

        [Test]
        public void Acquire_ResourceIsLockedTimesOut_ThrowsAnException()
        {
            // ARRANGE            
            bool lockAcquired1;
            DistributedLockTimeoutException exception = null;
            var realm = _storage.GetRealm();
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
            {
                lockAcquired1 = realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
                var t = new Thread(() =>
                {
                    exception = Assert.Throws<DistributedLockTimeoutException>(() =>
                        new RealmDistributedLock("resource1", TimeSpan.Zero, _storage));
                });
                t.Start();
                Assert.True(t.Join(5000), "Thread is hanging unexpected");
            }
            
            // ASSERT
            Assert.True(lockAcquired1);
            Assert.NotNull(exception);
        }

        [Test]
        public void Acquire_SignaledAtLockRelease_WaitForLock()
        {
            // ARRANGE
            var waitTimeBeforeLockAcquired = TimeSpan.MinValue;
            var t = new Thread(() =>
            {
                using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            });
            t.Start();

            // Wait just a bit to make sure the above lock is acquired
            Thread.Sleep(TimeSpan.FromSeconds(1));

            // ACT
            // Record when we try to acquire the lock
            var startTime = DateTime.UtcNow;
            using (new RealmDistributedLock("resource1", TimeSpan.FromSeconds(100), _storage))
            {
                waitTimeBeforeLockAcquired = DateTime.UtcNow - startTime;
            }
            
            // ASSERT
            Assert.That(waitTimeBeforeLockAcquired, Is.InRange(TimeSpan.Zero, TimeSpan.FromSeconds(5)));
        }

        [Test]
        public void Dispose_ResourceIsNotLocked_LockReleased()
        {
            // ARRANGE
            bool lockAcquired;
            bool lockReleased;
            var realm = _storage.GetRealm();
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
            {
                lockAcquired = realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
            }
 
            lockReleased = realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt == null) == 1;
            
            // ASSERT
            Assert.True(lockAcquired);
            Assert.True(lockReleased);
        }
        
        [Test]
        public void Acquire_ResourceIsNotLocked_LockExpireSet()
        {
            var realm = _storage.GetRealm();
            _storage.Options.DistributedLockLifetime = TimeSpan.FromSeconds(3);
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _storage))
            {
                var initialExpireAt = DateTime.UtcNow;
                Thread.Sleep(TimeSpan.FromSeconds(5));

                var lockEntry = realm.Find<LockDto>("resource1");
                Assert.NotNull(lockEntry);
                Assert.True(lockEntry.ExpireAt > initialExpireAt);
            }
            //reset
            _storage.Options.DistributedLockLifetime = TimeSpan.FromSeconds(30);
        }
    }
}