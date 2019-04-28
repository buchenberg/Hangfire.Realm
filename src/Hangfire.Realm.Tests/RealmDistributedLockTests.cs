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
        private IRealmDbContext _realmDbContext;
        private Realms.Realm _realm;
        
        [SetUp]
        public void Init()
        {
            _realmDbContext = new RealmDbContext(ConnectionUtils.GetRealmConfiguration());
            _realm = _realmDbContext.GetRealm();
		    
            _realm.Write(() => _realm.RemoveAll());
        }

        [Test]
        public void Acquire_ResourceIsNotLocked_LockAcquired()
        {
            // ARRANGE
            bool lockAcquired;
            
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions()))
            {
                lockAcquired = _realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
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
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions()))
            {
                lockAcquired1 = _realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
                using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext,
                    new RealmJobStorageOptions()))
                {
                    lockAcquired2 = _realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
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
            
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions()))
            {
                lockAcquired1 = _realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
                var t = new Thread(() =>
                {
                    exception = Assert.Throws<DistributedLockTimeoutException>(() =>
                        new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions()));
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
                using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions()))
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
            using (new RealmDistributedLock("resource1", TimeSpan.FromSeconds(100), _realmDbContext, new RealmJobStorageOptions()))
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
            // ACT
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions()))
            {
                lockAcquired = _realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt != null) == 1;
            }
 
            lockReleased = _realm.All<LockDto>().Count(l => l.Resource == "resource1" && l.ExpireAt == null) == 1;
            
            // ASSERT
            Assert.True(lockAcquired);
            Assert.True(lockReleased);
        }
        
        [Test]
        public void Acquire_ResourceIsNotLocked_LockExpireSet()
        {
            using (new RealmDistributedLock("resource1", TimeSpan.Zero, _realmDbContext, new RealmJobStorageOptions{DistributedLockLifetime = TimeSpan.FromSeconds(3)}))
            {
                var initialExpireAt = DateTime.UtcNow;
                Thread.Sleep(TimeSpan.FromSeconds(5));

                var lockEntry = _realm.Find<LockDto>("resource1");
                Assert.NotNull(lockEntry);
                Assert.True(lockEntry.ExpireAt > initialExpireAt);
            }
        }
    }
}