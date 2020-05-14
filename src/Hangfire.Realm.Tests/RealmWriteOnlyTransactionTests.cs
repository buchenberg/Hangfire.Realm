using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Realm.DAL;
using Hangfire.Realm.Models;
using Hangfire.Realm.Tests.Utils;
using Hangfire.States;
using Moq;
using NUnit.Framework;

namespace Hangfire.Realm.Tests
{
#pragma warning disable 1591

    [TestFixture]
    public class RealmWriteOnlyTransactionFacts
    {
        private Realms.Realm _realm;
        private RealmWriteOnlyTransaction _transaction;

        [SetUp]
        public void Init()
        {
            var storage = new RealmJobStorage(new RealmJobStorageOptions()
            {
                RealmConfiguration = ConnectionUtils.GetRealmConfiguration()
            });

            _realm = storage.GetRealm();
            _transaction = new RealmWriteOnlyTransaction(storage);
            _realm.Write(() => _realm.RemoveAll());

        }

        [TearDown]
        public void Cleanup()
        {
            _transaction.Dispose();
        }


        [Test]
        public void ExpireJob_OneDay_SetsJobExpirationData()
        {
            // ARRANGE
            JobDto jobDto = CreateEmptyJob();
            JobDto anotherJobDto = CreateEmptyJob();
            _realm.Write(() =>
            {
                _realm.Add(jobDto);
                _realm.Add(anotherJobDto);

            });



            var jobId = jobDto.Id;
            var anotherJobId = anotherJobDto.Id;

            // ACT
            _transaction.ExpireJob(jobId, TimeSpan.FromDays(1));
            _transaction.Commit();

            // ASSERT
            var testJob = _realm.Find<JobDto>(jobId);

            Assert.True(DateTime.UtcNow.AddMinutes(-1) < testJob.ExpireAt && testJob.ExpireAt <= DateTime.UtcNow.AddDays(1));

            var anotherTestJob = _realm.Find<JobDto>(anotherJobId);
            Assert.Null(anotherTestJob.ExpireAt);
        }

        [Test]
        public void PersistJob__ValidJobId_ClearsTheJobExpirationData()
        {
            // ARRANGE
            JobDto jobDto = CreateExpiredJob();
            JobDto anotherJobDto = CreateExpiredJob();
            _realm.Write(() =>
            {
                _realm.Add(jobDto);
                _realm.Add(anotherJobDto);

            });



            var jobId = jobDto.Id;
            var anotherJobId = anotherJobDto.Id;

            // ACT
            _transaction.PersistJob(jobId);
            _transaction.Commit();

            // ASSERT
            var testJob = _realm.Find<JobDto>(jobId);

            Assert.Null(testJob.ExpireAt);

            var anotherTestJob = _realm.Find<JobDto>(anotherJobId);
            Assert.NotNull(anotherTestJob.ExpireAt);
        }

        [Test]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            // ARRANGE
            JobDto jobDto = CreateEmptyJob();
            JobDto anotherJobDto = CreateEmptyJob();
            _realm.Write(() =>
            {
                _realm.Add(jobDto);
                _realm.Add(anotherJobDto);
            });


            var jobId = jobDto.Id;
            var anotherJobId = anotherJobDto.Id;

            var serializedData = new Dictionary<string, string> { { "Name", "Value" } };

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns("State");
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).Returns(serializedData);

            // ACT
            _transaction.SetJobState(jobId, state.Object);
            _transaction.Commit();

            // ASSERT
            var testJob = _realm.Find<JobDto>(jobId);
            Assert.AreEqual("State", testJob.StateName);
            Assert.AreEqual(1, testJob.StateHistory.Count);

            var anotherTestJob = _realm.Find<JobDto>(anotherJobId);
            Assert.Null(anotherTestJob.StateName);
            Assert.AreEqual(0, anotherTestJob.StateHistory.Count);

            var jobWithStates = _realm.All<JobDto>().FirstOrDefault();

            Assert.IsNotNull(jobWithStates);

            var jobState = jobWithStates.StateHistory.Single();
            Assert.AreEqual("State", jobState.Name);
            Assert.AreEqual("Reason", jobState.Reason);
            foreach (var valueDto in jobState.Data)
            {
                Assert.IsTrue(serializedData.ContainsKey(valueDto.Key));
                Assert.AreEqual(serializedData[valueDto.Key], valueDto.Value);
            }
        }

        [Test]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            // ARRANGE
            JobDto jobDto = CreateEmptyJob();
            _realm.Write(() =>
            {

                _realm.Add(jobDto);
            });


            var jobId = jobDto.Id;
            var serializedData = new Dictionary<string, string> { { "Name", "Value" } };

            var state = new Mock<IState>();
            state.Setup(x => x.Name).Returns("State");
            state.Setup(x => x.Reason).Returns("Reason");
            state.Setup(x => x.SerializeData()).Returns(serializedData);

            // ACT
            _transaction.AddJobState(jobId, state.Object);
            _transaction.Commit();

            var testJob = _realm.Find<JobDto>(jobId);

            // ASSERT
            var jobWithStates = _realm.All<JobDto>().Single();
            var jobState = jobWithStates.StateHistory.Last();
            Assert.Null(testJob.StateName);
            Assert.AreEqual("State", jobState.Name);
            Assert.AreEqual("Reason", jobState.Reason);
            Assert.AreEqual(serializedData, jobState.Data.ToDictionary(d => d.Key, d => d.Value));
        }

        [Test]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            //ARRANGE

            //ACT
            _transaction.AddToQueue("default", "1");
            _transaction.Commit();

            //ASSERT
            var testJob = _realm.All<JobQueueDto>().SingleOrDefault(j => j.JobId == "1");
            Assert.NotNull(testJob);
            Assert.AreEqual("default", testJob.Queue);
        }

        [Test]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            //ARRANGE

            //ACT
            _transaction.IncrementCounter("my-key");
            _transaction.Commit();

            //ASSERT
            CounterDto record = _realm.All<CounterDto>().Single();

            Assert.AreEqual("my-key", record.Key);
            Assert.AreEqual(1L, record.Value);
            Assert.Null(record.ExpireAt);
        }

        [Test]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            // ARRANGE

            //ACT
            _transaction.IncrementCounter("my-key", TimeSpan.FromDays(1));
            _transaction.Commit();

            //ASSERT
            CounterDto record = _realm.All<CounterDto>().Single();

            Assert.AreEqual("my-key", record.Key);
            Assert.AreEqual(1L, record.Value);
            Assert.NotNull(record.ExpireAt);

            var expireAt = (DateTimeOffset)record.ExpireAt;

            Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
            Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
        }

        [Test]
        public void IncrementCounter_WithExistingKey_IncrementsCounter()
        {
            // ARRANGE

            //ACT
            _transaction.IncrementCounter("my-key");
            _transaction.IncrementCounter("my-key");
            _transaction.Commit();

            //ASSERT
            var counter = _realm.All<CounterDto>().FirstOrDefault(k => k.Key == "my-key");

            Assert.NotNull(counter);
            Assert.AreEqual(2L, (long)counter.Value);
        }

        [Test]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            // ARRANGE

            // ACT
            _transaction.DecrementCounter("my-key");
            _transaction.Commit();

            // ASSERT
            var counter = _realm.All<CounterDto>().FirstOrDefault(k => k.Key == "my-key");

            Assert.NotNull(counter);
            Assert.AreEqual(-1L, (long)counter.Value);
        }

        [Test]
        public void DecrementCounter_WithExpiry_AddsARecordWithExpirationTimeSet()
        {
            // ARRANGE

            // ACT
            _transaction.DecrementCounter("my-key", TimeSpan.FromDays(1));
            _transaction.Commit();

            // ASSERT
            var counter = _realm.All<CounterDto>().Single();

            Assert.AreEqual("my-key", counter.Key);
            Assert.AreEqual(-1L, counter.Value);
            Assert.NotNull(counter.ExpireAt);

            var expireAt = counter.ExpireAt.Value.DateTime;

            Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
            Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
        }

        [Test]
        public void DecrementCounter_WithExistingKey_DecrementsCounter()
        {
            // ARRANGE

            // ACT
            _transaction.DecrementCounter("my-key");
            _transaction.DecrementCounter("my-key");
            _transaction.Commit();

            // ASSERT
            var counter = _realm.All<CounterDto>().Single();

            Assert.AreEqual(-2, (long)counter.Value);
        }

        [Test]
        public void AddToSet_NoSuchKeyAndValue_AddsARecord()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().Single();

            Assert.AreEqual("my-key", set.Key);
            Assert.AreEqual("my-value", set.Value);
            Assert.AreEqual(0.0, set.Score, 2);
        }

        [Test]
        public void AddToSet_KeyIsExistsButValuesAreDifferent_AddsARecord()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.AddToSet("my-key", "another-value");
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().ToList();

            Assert.AreEqual("my-key", set[0].Key);
            Assert.AreEqual("my-value", set[0].Value);
            Assert.AreEqual(0.0, set[0].Score, 2);
            Assert.AreEqual("another-value", set[1].Value);
            Assert.AreEqual(0.0, set[1].Score, 2);
        }

        [Test]
        public void AddToSet_BothKeyAndValueAreExist_DoesNotAddARecord()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.AddToSet("my-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().Single();

            Assert.AreEqual("my-key", set.Key);
            Assert.AreEqual("my-value", set.Value);
            Assert.AreEqual(0.0, set.Score, 2);
        }

        [Test]
        public void AddToSet_WithScore_BothKeyAndValueAreNotExist_AddsARecordWithScore()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value", 3.2);
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().Single();

            Assert.AreEqual("my-key", set.Key);
            Assert.AreEqual("my-value", set.Value);
            Assert.AreEqual(3.2, set.Score, 2);
        }

        [Test]
        public void AddToSet_BothKeyAndValueAreExistWithScore_UpdatesAScore()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.AddToSet("my-key", "my-value", 3.2);
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().Single();

            Assert.AreEqual("my-key", set.Key);
            Assert.AreEqual("my-value", set.Value);
            Assert.AreEqual(3.2, set.Score, 2);
        }

        [Test]
        public void RemoveFromSet_Exists_RemovesARecord()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.RemoveFromSet("my-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().SingleOrDefault();
            Assert.IsNull(set);
        }

        [Test]
        public void RemoveFromSet_WithSameKeyDifferentValue_DoesNotRemoveRecord()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.RemoveFromSet("my-key", "different-value");
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().Single();
            Assert.IsNotNull(set);
        }

        [Test]
        public void RemoveFromSet_WithSameValueDifferentKey_DoesNotRemoveRecord()
        {
            // ARRANGE

            // ACT
            _transaction.AddToSet("my-key", "my-value");
            _transaction.RemoveFromSet("different-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var set = _realm.All<SetDto>().Single();
            Assert.IsNotNull(set);
        }

        [Test]
        public void InsertToList_ValidInput_AddsARecord()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.IsNotNull(list);
        }

        [Test]
        public void InsertToList_BothKeyAndValueExist_AddsAnotherRecord()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "my-value");
            _transaction.InsertToList("my-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(2, list.Values.Count);
        }

        [Test]
        public void RemoveFromList_MultipleKeyAndValues_RemovesAllRecords()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "my-value");
            _transaction.InsertToList("my-key", "my-value");
            _transaction.RemoveFromList("my-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(0, list.Values.Count);
        }

        [Test]
        public void RemoveFromList_SameKeyButDifferentValue_DoesNotRemoveRecords()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "my-value");
            _transaction.RemoveFromList("my-key", "different-value");
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(1, list.Values.Count);
        }

        [Test]
        public void RemoveFromList_SameValueButDifferentKey_DoesNotRemoveRecords()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "my-value");
            _transaction.RemoveFromList("different-key", "my-value");
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(1, list.Values.Count);
        }

        [Test]
        public void TrimList_SpecifiedRange_TrimsAList()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "0");
            _transaction.InsertToList("my-key", "1");
            _transaction.InsertToList("my-key", "2");
            _transaction.InsertToList("my-key", "3");
            _transaction.TrimList("my-key", 1, 2);
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(2, list.Values.Count);
            Assert.AreEqual("1", list.Values[0]);
            Assert.AreEqual("2", list.Values[1]);
        }

        [Test]
        public void TrimList_KeepAndingAtGreaterThanMaxElementIndex_RemovesRecordsToEnd()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "0");
            _transaction.InsertToList("my-key", "1");
            _transaction.InsertToList("my-key", "2");
            _transaction.TrimList("my-key", 1, 100);
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(2, list.Values.Count);
        }

        [Test]
        public void TrimList_StartingFromValueGreaterThanMaxElementIndex_RemovesAllRecords()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "0");
            _transaction.TrimList("my-key", 1, 100);
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(0, list.Values.Count);
        }

        [Test]
        public void TrimList_StartFromGreaterThanEndingAt_RemovesAllRecords()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "0");
            _transaction.TrimList("my-key", 1, 0);
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(0, list.Values.Count);
        }

        [Test]
        public void TrimList_WrongKey_OnlyRemovesRecordsOfGivenKey()
        {
            // ARRANGE

            // ACT
            _transaction.InsertToList("my-key", "0");
            _transaction.TrimList("another-key", 1, 0);
            _transaction.Commit();

            // ASSERT
            var list = _realm.All<ListDto>().Single();
            Assert.AreEqual(1, list.Values.Count);
        }

        [Test]
        public void SetRangeInHash_KeyIsNull_ThrowsAnException()
        {
            // ARRANGE

            // ACT
            var exception = Assert.Throws<ArgumentNullException>(
                () =>
                {
                    _transaction.SetRangeInHash(null, new List<KeyValuePair<string, string>>());
                });

            // ASSERT
            Assert.AreEqual("key", exception.ParamName);
        }

        [Test]
        public void SetRangeInHash_KeyValuePairsArgumentIsNull_ThrowsAnException()
        {
            // ARRANGE

            // ACT
            var exception = Assert.Throws<ArgumentNullException>(
                () =>
                {
                    _transaction.SetRangeInHash("some-hash", null);
                });

            // ASSERT
            Assert.AreEqual("keyValuePairs", exception.ParamName);
        }

        [Test]
        public void SetRangeInHash_MultipleRecords_MergesAllRecords()
        {
            // ARRANGE

            // ACT
            _transaction.SetRangeInHash("some-hash", new Dictionary<string, string>
            {
                { "Key1", "Value1" },
                { "Key2", "Value2" }
            });

            // ASSERT
            var fields = _realm.All<HashDto>().ToList().Single().Fields.ToDictionary(f => f.Key, f => f.Value);
            Assert.AreEqual("Value1", fields["Key1"]);
            Assert.AreEqual("Value2", fields["Key2"]);
        }

        [Test]
        public void RemoveHash_KeyIsNull_ThrowsAnException()
        {
            Assert.Throws<ArgumentNullException>(
                () => _transaction.RemoveHash(null));
        }

        [Test]
        public void RemoveHash_ValidKey_RemovesHashRecords()
        {
            // ARRANGE
            _transaction.SetRangeInHash("some-hash", new Dictionary<string, string>
            {
                { "Key1", "Value1" },
                { "Key2", "Value2" }
            });

            // ACT
            _transaction.RemoveHash("some-hash");

            // ASSERT
            var hash = _realm.All<HashDto>().SingleOrDefault();
            Assert.IsNull(hash);
        }

        [Test]
        public void ExpireSet_SetExpirationDate_ExpirationDataSet()
        {
            // ARRANGE
            _realm.Write(() =>
            {
                _realm.Add(new SetDto { Key = "Set1", Value = "value1" });
                _realm.Add(new SetDto { Key = "Set2", Value = "value2" });
            });


            // ACT
            _transaction.ExpireSet("Set1", TimeSpan.FromDays(1));
            _transaction.Commit();

            // ASSERT
            var set1 = _realm.All<SetDto>().Where(_ => _.Key == "Set1" && _.Value == "value1").Single();
            var set2 = _realm.All<SetDto>().Where(_ => _.Key == "Set2" && _.Value == "value2").Single();
            Assert.IsNotNull(set1);
            Assert.NotNull(set1.ExpireAt);
            Assert.True(DateTimeOffset.UtcNow.AddMinutes(-1) < set1.ExpireAt && set1.ExpireAt <= DateTimeOffset.UtcNow.AddDays(1.5));
            Assert.IsNull(set2.ExpireAt);
        }

        [Test]
        public void ExpireList_SetExpirationDate_ExpirationDataSet()
        {
            // ARRANGE
            var list1 = new ListDto { Key = "List1" };
            list1.Values.Add("value1");
            var list2 = new ListDto { Key = "List2" };
            list1.Values.Add("value2");
            _realm.Write(() =>
            {
                _realm.Add(list1);
                _realm.Add(list2);
            });


            // ACT
            _transaction.ExpireList("List1", TimeSpan.FromDays(1));
            _transaction.Commit();

            // ASSERT
            list1 = _realm.Find<ListDto>("List1");
            list2 = _realm.Find<ListDto>("List2");
            Assert.IsNotNull(list1);
            Assert.NotNull(list1.ExpireAt);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < list1.ExpireAt && list1.ExpireAt <= DateTime.UtcNow.AddDays(1));
            Assert.IsNull(list2.ExpireAt);
        }

        [Test]
        public void ExpireHash_SetExpirationDate_ExpirationDataSet()
        {
            // ARRANGE
            _realm.Write(() =>
            {
                _realm.Add(new HashDto("Hash1"));
                _realm.Add(new HashDto("Hash2"));
            });
            // ACT
            //expire in one day
            _transaction.ExpireHash("Hash1", TimeSpan.FromDays(1));
            _transaction.Commit();

            // ASSERT
            var hash1 = _realm.Find<HashDto>("Hash1");
            var hash2 = _realm.Find<HashDto>("Hash2");
            Assert.IsNotNull(hash1);
            Assert.NotNull(hash1.ExpireAt);
            var startTime = DateTimeOffset.UtcNow.AddMinutes(-1);
            var endTime = DateTimeOffset.UtcNow.AddDays(1.5);
            Assert.True(startTime < hash1.ExpireAt);
            Assert.True(hash1.ExpireAt <= endTime);
            Assert.IsNull(hash2.ExpireAt);
        }

        [Test]
        public void PersistSet_OfGivenKey_ClearsTheSetExpirationData()
        {
            // ARRANGE
            var set1 = new SetDto("Set1", "value1", 0) { ExpireAt = DateTime.UtcNow };
            var set2 = new SetDto("Set2", "value2", 0) { ExpireAt = DateTime.UtcNow };
            _realm.Write(() =>
            {
                _realm.Add(set1);
                _realm.Add(set2);
            });
            
            // ACT
            _transaction.PersistSet(set1.Key);
            _transaction.Commit();

            // ASSERT
            var testSet1 = _realm.All<SetDto>().Where(_ => _.Key == "Set1" && _.Value == "value1").Single();
            Assert.Null(testSet1.ExpireAt);

            var testSet2 = _realm.All<SetDto>().Where(_ => _.Key == "Set2" && _.Value == "value2").Single();
            Assert.NotNull(testSet2.ExpireAt);
        }

        [Test]
        public void PersistList_OfGivenKey_ClearsTheListExpirationData()
        {
            // ARRANGE
            var list1 = new ListDto { Key = "List1", ExpireAt = DateTimeOffset.UtcNow };
            list1.Values.Add("value1");
            var list2 = new ListDto { Key = "List2", ExpireAt = DateTimeOffset.UtcNow };
            list1.Values.Add("value2");
            _realm.Write(() =>
            {
                _realm.Add(list1);
                _realm.Add(list2);
            });


            // ACT
            _transaction.PersistList(list1.Key);
            _transaction.Commit();

            // ASSERT
            var testSet1 = _realm.Find<ListDto>(list1.Key);
            Assert.Null(testSet1.ExpireAt);

            var testSet2 = _realm.Find<ListDto>(list2.Key);
            Assert.NotNull(testSet2.ExpireAt);
        }

        [Test]
        public void PersistHash_OfGivenKey_ClearsTheHashExpirationData()
        {
            // ARRANGE
            var hash1 = new HashDto { Key = "Hash1", ExpireAt = DateTimeOffset.UtcNow };
            var hash2 = new HashDto { Key = "Hash2", ExpireAt = DateTimeOffset.UtcNow };
            _realm.Write(() =>
            {
                _realm.Add(hash1);
                _realm.Add(hash2);
            });


            // ACT
            _transaction.PersistHash(hash1.Key);
            _transaction.Commit();

            // ASSERT
            var testSet1 = _realm.Find<HashDto>(hash1.Key);
            Assert.Null(testSet1.ExpireAt);

            var testSet2 = _realm.Find<HashDto>(hash2.Key);
            Assert.NotNull(testSet2.ExpireAt);
        }

        [Test]
        public void AddRangeToSet_SetExists_AddToExistingSetData()
        {
            // ARRANGE
            var set1Val1 = new SetDto { Key = "Set1<value1>", Value = "value1", ExpireAt = DateTimeOffset.UtcNow };
            _realm.Write(() =>
            {
                _realm.Add(set1Val1);
            });


            var set1Val2 = new SetDto { Key = "Set1<value2>", Value = "value2", ExpireAt = DateTimeOffset.UtcNow };
            _realm.Write(() =>
            {
                _realm.Add(set1Val2);
            });


            var set2 = new SetDto { Key = "Set2", Value = "value2", ExpireAt = DateTimeOffset.UtcNow };
            _realm.Write(() =>
            {
                _realm.Add(set2);
            });


            var values = new[] { "test1", "test2", "test3" };

            // ACT
            _transaction.AddRangeToSet(set1Val1.Key, values);
            _transaction.Commit();

            // ASSERT

            var testSet1 = _realm.All<SetDto>().Where(s => s.Key.StartsWith("Set1")).ToList();
            var valuesToTest = new List<string>(values) { "value1", "value2" };

            Assert.NotNull(testSet1);
            // verify all values are present in testSet1
            Assert.True(testSet1.Select(s => s.Value.ToString()).All(value => valuesToTest.Contains(value)));
            Assert.AreEqual(5, testSet1.Count);

            var testSet2 = _realm.All<SetDto>().Where(s => s.Key.StartsWith("Set2")).ToList();
            Assert.NotNull(testSet2);
            Assert.AreEqual(1, testSet2.Count);
        }

        [Test]
        public void RemoveSet_ByKey_ClearsTheSetData()
        {
            // ARRANGE
            _realm.Write(() =>
            {
                var set1Val1 = new SetDto { Key = "Set1<value1>", Value = "value1", ExpireAt = DateTimeOffset.UtcNow };
                _realm.Add(set1Val1);

                var set1Val2 = new SetDto { Key = "Set1<value2>", Value = "value2", ExpireAt = DateTimeOffset.UtcNow };
                _realm.Add(set1Val2);

                var set2 = new SetDto { Key = "Set2", Value = "value2", ExpireAt = DateTimeOffset.UtcNow };
                _realm.Add(set2);

            });

            // ACT
            _transaction.RemoveSet("Set1");

            // ASSERT
            var testSet1 = _realm.All<SetDto>().Where(s => s.Key.StartsWith("Set1")).ToList();
            Assert.AreEqual(0, testSet1.Count);

            var testSet2 = _realm.All<SetDto>().Where(s => s.Key.StartsWith("Set2")).ToList();
            Assert.AreEqual(1, testSet2.Count);
        }


        private JobDto CreateEmptyJob()
        {
            JobDto jobDto = new JobDto
            {
                Id = Guid.NewGuid().ToString(),
                InvocationData = "",
                Arguments = "",
                Created = DateTime.UtcNow
            };
            return jobDto;
        }

        private JobDto CreateExpiredJob()
        {
            return CreateExpiredJob(DateTimeOffset.UtcNow);
        }
        private JobDto CreateExpiredJob(DateTimeOffset expire)
        {
            JobDto jobDto = CreateEmptyJob();
            jobDto.ExpireAt = expire;
            return jobDto;
        }
    }
#pragma warning restore 1591
}