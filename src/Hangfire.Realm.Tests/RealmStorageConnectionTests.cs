using System;
using System.Linq;
using System.Threading;
using Hangfire.Realm.Models;
using Hangfire.Realm.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using NUnit.Framework;

namespace Hangfire.Realm.Tests
{
    [TestFixture]
    public class RealmStorageConnectionTests
    {

        private IRealmDbContext _realmDbContext;
        private RealmStorageConnection _connection;
        private Realms.Realm _realm;

        [SetUp]
        public void Init()
        {
            _realmDbContext = new RealmDbContext(ConnectionUtils.GetRealmConfiguration());
            _connection = new RealmStorageConnection(_realmDbContext, new RealmJobStorageOptions());
            _realm = _realmDbContext.GetRealm();
            _realm.Write(() => _realm.RemoveAll());
        }

        [TearDown]
        public void Cleanup()
        {

        }

        [Test]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            // ARRANGE
            var context1 = new ServerContext
            {
                Queues = new[] { "critical", "default" },
                WorkerCount = 4
            };

            var context2 = new ServerContext
            {
                Queues = new[] { "default" },
                WorkerCount = 1000
            };

            // ACT - Create
            _connection.AnnounceServer("server", context1);

            // ASSERT
            var server = _realm.Find<ServerDto>("server");
            Assert.AreEqual("server", server.Id);
            Assert.AreEqual(context1.WorkerCount, server.WorkerCount);
            Assert.AreEqual(context1.Queues, server.Queues);
            Assert.NotNull(server.StartedAt);
            Assert.NotNull(server.LastHeartbeat);

            // ACT - Update
            _connection.AnnounceServer("server", context2);

            // ASSERT
            var sameServer = _realm.Find<ServerDto>("server");
            Assert.AreEqual("server", sameServer.Id);
            Assert.AreEqual(context2.WorkerCount, sameServer.WorkerCount);
        }

        [Test]
        public void RemoveServer_RemovesAServerRecord()
        {
            _realm.Write(() =>
            {
                var server1 = new ServerDto
                {
                    Id = "server1",
                    LastHeartbeat = DateTime.UtcNow
                };

                var server2 = new ServerDto
                {
                    Id = "server2",
                    LastHeartbeat = DateTime.UtcNow
                };

                _realm.Add(server1);
                _realm.Add(server2);
            });


            _connection.RemoveServer("server1");

            var servers = _realm.All<ServerDto>();
            Assert.IsTrue(servers.Any(s => s.Id == "server2"));
            Assert.IsFalse(servers.Any(s => s.Id == "server1"));

        }
    }
}
