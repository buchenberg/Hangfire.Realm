using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Hangfire.Common;
using Hangfire.Realm.Models;
using Hangfire.Realm.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using NUnit.Framework;
using Realms;

namespace Hangfire.Realm.Tests
{
    [TestFixture]
    public class RealmMonitoringApiTests
    {
		private const string DefaultQueue = "default";
		private const string FetchedStateName = "Fetched";
	    private const int From = 0;
	    private const int PerPage = 5;
	    
		private RealmMonitoringApi _monitoringApi;
        private IRealmDbContext _realmDbContext;
        private Realms.Realm _realm;

	    [SetUp]
	    public void Init()
	    {
            var storage = new RealmJobStorage(new RealmJobStorageOptions
            {
                RealmConfiguration = ConnectionUtils.GetRealmConfiguration()
            });
            _realmDbContext = new RealmDbContext(ConnectionUtils.GetRealmConfiguration());
		    _realm = _realmDbContext.GetRealm();
		    
		    _realm.Write(() => _realm.RemoveAll());
		    _monitoringApi = new RealmMonitoringApi(storage);
	    }

	    [TearDown]
	    public void Cleanup()
	    {
            _realm.Dispose();
	    }

        
        [Test]
        public void GetStatistics_NoJobsExist_ReturnsZero()
        {
            // ARRANGE
            
            // ACT
            var result = _monitoringApi.GetStatistics();

            // ASSERT
            Assert.AreEqual(0, result.Enqueued);
            Assert.AreEqual(0, result.Failed);
            Assert.AreEqual(0, result.Processing);
            Assert.AreEqual(0, result.Scheduled);
        }

		[Test]
		public void GetStatistics_JobsExist_ReturnsExpectedCounts()
		{
			// ARRANGE
			CreateJobInState(EnqueuedState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(1)));
			CreateJobInState(EnqueuedState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(2)));
			CreateJobInState(FailedState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(3)));
			CreateJobInState(ProcessingState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(4)));
			CreateJobInState(ScheduledState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(5)));
			CreateJobInState(ScheduledState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(6)));

			// ACT
			var result = _monitoringApi.GetStatistics();

			// ASSERT
			Assert.AreEqual(2, result.Enqueued);
			Assert.AreEqual(1, result.Failed);
			Assert.AreEqual(1, result.Processing);
			Assert.AreEqual(2, result.Scheduled);
		}

		[Test]
		public void JobDetails_NoJob_ReturnsNull()
		{
			// ARRANGE

			// ACT
			var result = _monitoringApi.JobDetails(Guid.NewGuid().ToString());

			// ASSERT
			Assert.Null(result);
		}
	    
	    [Test]
	    public void JobDetails_JobExists_ReturnsResult()
	    {
		    // ARRANGE
		    var job1 = CreateJobInState( EnqueuedState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(1)));

		    // ACT
		    var result = _monitoringApi.JobDetails(job1.Id);

		    // ASSERT
		    Assert.NotNull(result);
		    Assert.NotNull(result.Job);
		    Assert.AreEqual("Arguments", result.Job.Args[0]);
		    Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
		    Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
	    }
	    
	    [Test]
	    public void EnqueuedJobs_NoJobs_ReturnsEmpty()
	    {
		    // ARRANGE
		    
		    // ACT
		    var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

		    // ASSERT
		    Assert.IsEmpty(resultList);
	    }
	    
	    [Test]
	    public void EnqueuedJobs_OneJobExistsThatIsNotFetched_ReturnsSingleJob()
	    {
		    // ARRANGE
		    CreateJobInState(EnqueuedState.StateName, DateTime.UtcNow.Subtract(TimeSpan.FromSeconds(1)));

		    // ACT
		    var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

		    // ASSERT
		    Assert.AreEqual(1, resultList.Count);
	    }
	    
	    [Test]
	    public void EnqueuedJobs_OneJobExistsThatIsFetched_ReturnsEmpty()
	    {
		    // ARRANGE
		    CreateJobInState(FetchedStateName);

		    // ACT
		    var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

		    // ASSERT
		    Assert.IsEmpty(resultList);
	    }
	    
	    [Test]
	    public void EnqueuedJobs_MultipleJobsExistsInFetchedAndUnfetchedStates_ReturnsUnfetchedJobsOnly()
	    {
		    // ARRANGE
		    CreateJobInState( EnqueuedState.StateName);
		    CreateJobInState( EnqueuedState.StateName);
		    CreateJobInState(FetchedStateName);

		    // ACT
		    var resultList = _monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

		    // ASSERT
		    Assert.AreEqual(2, resultList.Count);
	    }
	    
	    [Test]
	    public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
	    {
		    // ARRANGE
		    
		    // ACT
		    var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

		    // ASSERT
		    Assert.IsEmpty(resultList);
	    }
	    
	    [Test]
	    public void FetchedJobs_OneJobExistsThatIsFetched_ReturnsSingleJob()
	    {
		    // ARRANGE
		    CreateJobInState(FetchedStateName);

		    var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

		    Assert.AreEqual(1, resultList.Count);
	    }
	    
	    [Test]
	    public void FetchedJobs_OneJobExistsThatIsNotFetched_ReturnsEmpty()
	    {
		    CreateJobInState(EnqueuedState.StateName);

		    var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

		    Assert.IsEmpty(resultList);
	    }
	    
	    [Test]
	    public void FetchedJobs_MultipleJobsExistsInFetchedAndUnfetchedStates_ReturnsFetchedJobsOnly()
	    {
		    CreateJobInState(FetchedStateName);
		    CreateJobInState(FetchedStateName);
		    CreateJobInState(EnqueuedState.StateName);

		    var resultList = _monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

		    Assert.AreEqual(2, resultList.Count);
	    }
	    
	    [Test]
	    public void FetchedJobs_MultipleJobsPaged_ReturnsCorrectJobsOnly()
	    {
		    // ARRANGE
		    var job1 = CreateJobInState(FetchedStateName, created: DateTime.Now.Subtract(TimeSpan.FromSeconds(6))); //first
		    var job2 = CreateJobInState(FetchedStateName, created: DateTime.Now.Subtract(TimeSpan.FromSeconds(5)));
		    var job3 = CreateJobInState(FetchedStateName, created: DateTime.Now.Subtract(TimeSpan.FromSeconds(4)));
		    var job4 = CreateJobInState(FetchedStateName, created: DateTime.Now.Subtract(TimeSpan.FromSeconds(3)));
		    var job5 = CreateJobInState(FetchedStateName, created: DateTime.Now.Subtract(TimeSpan.FromSeconds(2)));
		    var job6 = CreateJobInState(FetchedStateName, created: DateTime.Now.Subtract(TimeSpan.FromSeconds(1))); // last

		    // ACT 
		    var resultList = _monitoringApi.FetchedJobs(DefaultQueue, from: 2, perPage: 3);

		    // ASSERT (skip the two newest, take 3)
		    Assert.AreEqual(3, resultList.Count);
		    Assert.AreEqual(job2.Id, resultList[0].Key);
		    Assert.AreEqual(job3.Id, resultList[1].Key);
		    Assert.AreEqual(job4.Id, resultList[2].Key);
	    }
	    
	    [Test]
	    public void ProcessingJobs_MultipleJobsExistsInProcessingSucceededAndEnqueuedState_ReturnsProcessingJobsOnly()
	    {
		    // ARRANGE
		    CreateJobInState(ProcessingState.StateName);

		    CreateJobInState(SucceededState.StateName, visitor: jobDto =>
		    {
			    var processingState = new StateDto()
			    {
				    Name = ProcessingState.StateName,
				    Reason = null
			    };
				    
			    processingState.Data.Add(new StateDataDto("ServerId", Guid.NewGuid().ToString()));
			    processingState.Data.Add(new StateDataDto("StartedAt",
				    JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))));
			    
			    jobDto.StateHistory.Insert(0, processingState);
		    });

		    CreateJobInState(EnqueuedState.StateName);

		    // ACT
		    var resultList = _monitoringApi.ProcessingJobs(From, PerPage);

		    Assert.AreEqual(1, resultList.Count);
	    }
	    
	    [Test]
	    public void FailedJobs_InDescendingOrder_ReturnsFailedJobs()
	    {
		    // ARRANGE
		    var failedJob0 = CreateJobInState(FailedState.StateName);
		    var failedJob1 = CreateJobInState(FailedState.StateName);
		    var failedJob2 = CreateJobInState(FailedState.StateName);

		    // ACT
		    var resultList = _monitoringApi.FailedJobs(From, PerPage);

		    // ASSERT
		    Assert.AreEqual(3, resultList.Count);
		    Assert.AreEqual(failedJob0.Id, resultList[2].Key);
		    Assert.AreEqual(failedJob1.Id, resultList[1].Key);
		    Assert.AreEqual(failedJob2.Id, resultList[0].Key);
	    }
	    
	    [Test]
	    public void SucceededByDatesCount_ForLastWeek_ReturnsSuccededJobs()
	    {
		    // ARRANGE
		    var date = DateTime.UtcNow.Date;
		    var succededCount = 10L;
			_realm.Write(() => _realm.Add(new CounterDto
			{
				// this might fail if we test during date change... seems unlikely
				// TODO, wrap Datetime in a mock friendly wrapper
				Key = $"stats:succeeded:{date:yyyy-MM-dd}",
				Value = succededCount
			}));
		    
		    // ACT
		    var results = _monitoringApi.SucceededByDatesCount();

		    // ASSERT
		    Assert.AreEqual(succededCount, results[date]);
		    Assert.AreEqual(8, results.Count);
	    }
	    
	    [Test]
	    public void HourlySucceededJobs_ForLast24Hours_ReturnsSuccededJobs()
	    {
		    // ARRANGE
		    var now = DateTime.UtcNow;
		    var succeededCount = 10L;
			_realm.Write(() => _realm.Add(new CounterDto
			{
				// TODO, wrap Datetime in a mock friendly wrapper
				Key = $"stats:succeeded:{now:yyyy-MM-dd-HH}",
				Value = succeededCount
			}));
		    
		    // ACT
		    var results = _monitoringApi.HourlySucceededJobs();

		    // ASSERT
		    Assert.AreEqual(succeededCount, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
		    Assert.AreEqual(24, results.Count);
	    }
	    
	    [Test]
	    public void FailedByDatesCount_ForLastWeek_ReturnsFailedJobs()
	    {
		    // ARRANGE
		    var date = DateTime.UtcNow.Date;
		    var failedCount = 10L;

		    _realm.Write(() => _realm.Add(new CounterDto
		    {
			    // this might fail if we test during date change... seems unlikely
			    Key = $"stats:failed:{date:yyyy-MM-dd}",
			    Value = failedCount
		    }));
		    
		    // ACT
		    var results = _monitoringApi.FailedByDatesCount();

		    // ASSERT
		    Assert.AreEqual(failedCount, results[date]);
		    Assert.AreEqual(8, results.Count);
	    }
	    
	    [Test]
	    public void HourlyFailedJobs_ForLast24Hours_ReturnsFailedJobs()
	    {
		    // ARRANGE
		    var now = DateTime.UtcNow;
		    var failedCount = 10L;
		    
		    _realm.Write(() => _realm.Add(new CounterDto
		    {
			    // TODO, wrap Datetime in a mock friendly wrapper
			    Key = $"stats:failed:{now:yyyy-MM-dd-HH}",
			    Value = failedCount
		    }));
		    
		    // ACT
		    var results = _monitoringApi.HourlyFailedJobs();

		    // ASSERT
		    Assert.AreEqual(failedCount, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
		    Assert.AreEqual(24, results.Count);
	    }
	    
		private JobDto CreateJobInState(string stateName, DateTime created = default(DateTime), Action<JobDto> visitor = null)
		{
			var job = Common.Job.FromExpression(() => HangfireTestJobs.SampleMethod("wrong"));

			if (created == default(DateTime))
			{
				created = DateTime.Now;
			}
			
			Dictionary<string, string> stateData;

			if (stateName == EnqueuedState.StateName)
			{
				stateData = new Dictionary<string, string> { ["EnqueuedAt"] = $"{DateTime.UtcNow:o}" };
			}
			else if (stateName == ProcessingState.StateName)
			{
				stateData = new Dictionary<string, string>
				{
					["ServerId"] = Guid.NewGuid().ToString(),
					["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))
				};
			}
			else if (stateName == FailedState.StateName)
			{
				stateData = new Dictionary<string, string>
				{
					["ExceptionDetails"] = "Test_ExceptionDetails",
					["ExceptionMessage"] = "Test_ExceptionMessage",
					["ExceptionType"] = "Test_ExceptionType",
					["FailedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(10)))
				};
			}
			else
			{
				stateData = new Dictionary<string, string>();
			}

			var jobState = new StateDto()
			{
				Name = stateName,
				Reason = null
			};
			foreach (var item in stateData)
			{
				jobState.Data.Add(new StateDataDto(item));
			}
			
			var jobDto = new JobDto
			{
				Id = Guid.NewGuid().ToString(),
				Created = created,
				InvocationData = SerializationHelper.Serialize(InvocationData.SerializeJob(job)),
				Arguments = "[\"\\\"Arguments\\\"\"]",
				StateName = stateName
			};
			jobDto.StateHistory.Add(jobState);
			
			visitor?.Invoke(jobDto);
			
			_realm.Write(() =>_realm.Add(jobDto));
			
			var jobQueueDto = new JobQueueDto
			{
				Id = Guid.NewGuid().ToString(),
				FetchedAt = null,
				JobId = jobDto.Id,
				Queue = DefaultQueue
			};

			if (stateName == FetchedStateName)
			{
				jobQueueDto.FetchedAt = DateTime.UtcNow;
			}
			_realm.Write(() => _realm.Add(jobQueueDto));

			return jobDto;
		}
	}
}