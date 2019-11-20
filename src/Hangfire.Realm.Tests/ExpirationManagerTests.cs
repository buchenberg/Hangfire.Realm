using Hangfire.Realm.Models;
using Hangfire.Realm.Tests.Utils;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Hangfire.Realm.Tests
{
    [TestFixture]
    public class ExpirationManagerTests
    {
        private RealmJobStorage _storage;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

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
        public void RemovesOutdatedRecords()
        {
            CreateExpirationEntry(DateTimeOffset.UtcNow.AddMonths(-1));

            var manager = CreateManager();
            manager.Execute(_cts.Token);
            
            Assert.True(IsEntryExpired());
            
        }

        [Test]
        public void DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {

                CreateExpirationEntry(null);
                var manager = CreateManager();

                manager.Execute(_cts.Token);

                Assert.False(IsEntryExpired());
            
        }

        private bool IsEntryExpired()
        {
            var realm = _storage.GetRealm();
            var count = realm.All<JobDto>().Where(_ => _.Id == "some-test-job").Count();
            return count == 0;
        }

        private void CreateExpirationEntry(DateTimeOffset? expireAt)
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                realm.Add<JobDto>(new JobDto()
                {
                    Id = "some-test-job",
                    ExpireAt = expireAt
                });
            });
        }

        private ExpirationManager CreateManager()
        {
            return new ExpirationManager(_storage, TimeSpan.Zero);
        }
    }
}
