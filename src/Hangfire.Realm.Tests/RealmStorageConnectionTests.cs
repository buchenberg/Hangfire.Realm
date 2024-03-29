﻿using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Realm.DAL;
using Hangfire.Realm.Models;
using Hangfire.Realm.Tests.Utils;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using NUnit.Framework;
using Realms;

namespace Hangfire.Realm.Tests
{
    [TestFixture]
    public class RealmStorageConnectionTests
    {
        private RealmStorageConnection _connection;
        private RealmJobStorage _storage;

        [SetUp]
        public void Init()
        {
            _storage = new RealmJobStorage(new RealmJobStorageOptions()
            {
                RealmConfiguration = ConnectionUtils.GetRealmConfiguration()
            });
            _connection = new RealmStorageConnection(_storage);
            var realm = _storage.GetRealm();
            realm.Write(() => realm.RemoveAll());
        }

        [TearDown]
        public void Cleanup()
        {

        }

        [Test]
        public void AcquireLock_ReturnsNullInstance()
        {
            //No locks in RealmDb
            var @lock = _connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
            Assert.Null(@lock);
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
            var realm = _storage.GetRealm();

            // ACT - Create
            _connection.AnnounceServer("server", context1);

            // ASSERT
            var server = realm.Find<ServerDto>("server");
            Assert.AreEqual("server", server.Id);
            Assert.AreEqual(context1.WorkerCount, server.WorkerCount);
            Assert.AreEqual(context1.Queues, server.Queues);
            Assert.NotNull(server.StartedAt);
            Assert.NotNull(server.LastHeartbeat);

            // ACT - Update
            _connection.AnnounceServer("server", context2);

            // ASSERT
            var sameServer = realm.Find<ServerDto>("server");
            Assert.AreEqual("server", sameServer.Id);
            Assert.AreEqual(context2.WorkerCount, sameServer.WorkerCount);
        }

        [Test]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            var hash1 = new HashDto("some-hash");
            hash1.Fields.Add(new FieldDto("Key1","Value1"));
            hash1.Fields.Add(new FieldDto("Key2", "Value2"));

            var hash2 = new HashDto("another-hash");
            hash2.Fields.Add(new FieldDto("Key3", "Value3"));
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                realm.Add(hash1);
                realm.Add(hash2);
            });


            // Act
            var result = _connection.GetAllEntriesFromHash("some-hash");

            // Assert
            Assert.NotNull(result);
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("Value1", result["Key1"]);
            Assert.AreEqual("Value2", result["Key2"]);
            
        }
        [Test]
        public void SetRangeInHash_MergesAllRecords()
        {

            var fieldDictionary = new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                };
            var realm = _storage.GetRealm();
            
            _connection.SetRangeInHash("some-hash", fieldDictionary);

            var fieldLists = realm.All<HashDto>()
                .Where(_ => _.Key == "some-hash")
                .ToList()
                .Select(_ => _.Fields);
            var result = new Dictionary<string, string>();
            foreach (var fieldList in fieldLists)
            {
                foreach (var field in fieldList)
                {
                    result.Add(field.Key, field.Value);
                }

            }

            Assert.AreEqual("Value1", result["Key1"]);
            Assert.AreEqual("Value2", result["Key2"]);

        }
        [Test]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            var realm = _storage.GetRealm();
            var hash1 = new HashDto("hash-1");
            hash1.Fields.Add(new FieldDto("field-1", "1"));

            var hash2 = new HashDto("hash-2");
            hash2.Fields.Add(new FieldDto("field-2", "2"));

            var hash3 = new HashDto("hash-3");
            hash3.Fields.Add(new FieldDto("field-1", "3"));                                    

            realm.Write(() =>
            {
                realm.Add(hash1);
                realm.Add(hash2);
                realm.Add(hash3);
            });

            // Act
            var result = _connection.GetValueFromHash("hash-1", "field-1");

            // Assert
            Assert.AreEqual("1", result);
        }
        [Test]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
            var jobId = _connection.CreateExpiredJob(
                Job.FromExpression(() => HangfireTestJobs.SampleMethod("Hello")),
                new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                createdAt,
                TimeSpan.FromDays(1));
            var realm = _storage.GetRealm();

            Assert.NotNull(jobId);
            Assert.IsNotEmpty(jobId);

            var databaseJob = realm.Find<JobDto>(jobId);
            Assert.AreEqual(jobId, databaseJob.Id.ToString());
            Assert.AreEqual(createdAt, databaseJob.Created.DateTime);
            Assert.Null(databaseJob.StateName);

            var invocationData = SerializationHelper.Deserialize<InvocationData>(databaseJob.InvocationData);
            invocationData.Arguments = databaseJob.Arguments;

            var job = invocationData.DeserializeJob();
            Assert.AreEqual(typeof(HangfireTestJobs), job.Type);
            Assert.AreEqual(nameof(HangfireTestJobs.SampleMethod), job.Method.Name);
            Assert.AreEqual("Hello", job.Args[0]);

            Assert.True(createdAt.AddDays(1).AddMinutes(-1) < databaseJob.ExpireAt);
            Assert.True(databaseJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));

            var parameters = realm.Find<JobDto>(jobId).Parameters;
            Dictionary<string, string> paramDictionary = parameters.ToDictionary(_ => _.Key, _ => _.Value);

            Assert.NotNull(parameters);
            Assert.AreEqual("Value1", paramDictionary["Key1"]);
            Assert.AreEqual("Value2", paramDictionary["Key2"]);
        }

        [Test]
        public void GetStateData_ReturnsCorrectData()
        {
            var data = new StateDataDto
            {
                Key = "Key",
                Value = "Value"
            };
            var state = new StateDto
            {
                Name = "old-state",
                Created = DateTime.UtcNow
            };
            var jobDto = new JobDto
            {
                Id = Guid.NewGuid().ToString(),
                InvocationData = "",
                Arguments = "",
                StateName = "",
                Created = DateTime.UtcNow
            };
            var realm = _storage.GetRealm();


            var stateUpdate = new StateDto
            {
                Name = "Name",
                Reason = "Reason",
                Created = DateTime.UtcNow
            };

            realm.Write(() =>
            {
                jobDto.StateHistory.Add(state);
                realm.Add(jobDto);
                stateUpdate.Data.Add(data);
                jobDto.StateHistory.Add(stateUpdate);
                realm.Add(jobDto, update: true);
            });

            

            var result = _connection.GetStateData(jobDto.Id);
            Assert.NotNull(result);
            Assert.AreEqual("Name", result.Name);
            Assert.AreEqual("Reason", result.Reason);
            Assert.AreEqual("Value", result.Data["Key"]);
        }

        [Test]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            var job = Job.FromExpression(() => HangfireTestJobs.SampleMethod("wrong"));

            var jobDto = new JobDto
            {
                Id = Guid.NewGuid().ToString(),
                InvocationData = SerializationHelper.Serialize(InvocationData.SerializeJob(job)),
                Arguments = "[\"\\\"Arguments\\\"\"]",
                StateName = SucceededState.StateName,
                Created = DateTime.UtcNow
            };
            var realm = _storage.GetRealm();
            realm.Write(() => { realm.Add(jobDto); });

            var result = _connection.GetJobData(jobDto.Id.ToString());

            Assert.NotNull(result);
            Assert.NotNull(result.Job);
            Assert.AreEqual(SucceededState.StateName, result.State);
            Assert.AreEqual("Arguments", result.Job.Args[0]);
            Assert.Null(result.LoadException);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
            Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }
        
        [Test]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => _connection.Heartbeat(null));
        }

        [Test]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var server1 = new ServerDto
                {
                    Id = "server1",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                };

                var server2 = new ServerDto
                {
                    Id = "server2",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                };

                realm.Add(server1);
                realm.Add(server2);
            });

            _connection.Heartbeat("server1");

            var servers = realm.All<ServerDto>().ToList()
                .ToDictionary(x => x.Id, x => x.LastHeartbeat);

            Assert.True(servers.ContainsKey("server1"));
            Assert.True(servers.ContainsKey("server2"));
            Assert.AreNotEqual(2012, servers["server1"].Value.Year);
            Assert.AreEqual(2012, servers["server2"].Value.Year);
        }

        [Test]
        public void RemoveServer_RemovesAServerRecord()
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                var server1 = new ServerDto
                {
                    Id = "server1",
                    LastHeartbeat = DateTime.UtcNow
                };
                realm.Add(server1);

                var server2 = new ServerDto
                {
                    Id = "server2",
                    LastHeartbeat = DateTime.UtcNow
                };
                realm.Add(server2);
            });


            _connection.RemoveServer("server1");

            var servers = realm.All<ServerDto>();
            Assert.IsTrue(servers.Any(s => s.Id == "server2"));
            Assert.IsFalse(servers.Any(s => s.Id == "server1"));

        }

        [Test]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.RemoveServer(null));
        }

        [Test]
        public void RemoveTimedOutServers_RemovesServers()
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                realm.Add(new ServerDto
                {
                    Id = "server1",
                    LastHeartbeat = DateTime.UtcNow.AddDays(-1)
                });
                realm.Add(new ServerDto
                {
                    Id = "server2",
                    LastHeartbeat = DateTime.UtcNow.AddHours(-12)
                });
                realm.Add(new ServerDto
                {
                    Id = "server3",
                    LastHeartbeat = DateTime.UtcNow.AddHours(-17)
                });
            });


            var deletedServerCount = _connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

            var liveServer = realm.All<ServerDto>().FirstOrDefault();
            Assert.AreEqual("server2", liveServer.Id);
            Assert.AreEqual(2, deletedServerCount);
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            var realm = _storage.GetRealm();
            realm.Write(() =>
            {
                realm.Add(new SetDto
                {
                    Key = "key",
                    Value = "1.0",
                    Score = 1.0
                });
                realm.Add(new SetDto
                {
                    Key = "key",
                    Value = "-5.0",
                    Score = -5.0
                });
                realm.Add(new SetDto
                {
                    Key = "key",
                    Value = "-1.0",
                    Score = -1.0
                });
                realm.Add(new SetDto
                {
                    Key = "key",
                    Value = "-2.0",
                    Score = -2.0
                });
            });

            var result = _connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

            Assert.AreEqual("-1.0", result);
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {

                var result = _connection.GetFirstByLowestScoreFromSet(
                    "key", 0, 1);
                Assert.Null(result);

        }
    }
}
