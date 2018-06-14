using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Realm.RealmObjects;
using Hangfire.Realm.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using NUnit.Framework;

namespace Hangfire.Realm.Tests
{
    [TestFixture]
    public class RealmMonitoringApiFacts
    {
		private const string DefaultQueue = "default";
		private const string FetchedStateName = "Fetched";
	    private const int From = 0;
	    private const int PerPage = 5;
	    
		private RealmMonitoringApi _monitoringApi;
        private Realms.Realm _realm;

	    [SetUp]
	    public void Init()
	    {
		    _realm = ConnectionUtils.GetRealm();
		    
		    _realm.Write(() => _realm.RemoveAll());
		    _monitoringApi = new RealmMonitoringApi(_realm);
	    }

	    [TearDown]
	    public void Cleanup()
	    {
		    
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
				    Reason = null,
				    Created = DateTime.UtcNow,
			    };
				    
			    processingState.Data.Add(new KeyValueDto("ServerId", Guid.NewGuid().ToString()));
			    processingState.Data.Add(new KeyValueDto("StartedAt",
				    JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))));
			    
			    jobDto.StateHistory.Insert(0, processingState);
		    });

		    CreateJobInState(EnqueuedState.StateName);

		    // ACT
		    var resultList = _monitoringApi.ProcessingJobs(From, PerPage);

		    Assert.AreEqual(1, resultList.Count);
	    }
	    
		#if false

		[Fact, CleanDatabase]
		public void FailedJobs_ReturnsFailedJobs_InDescendingOrder()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var failedJob0 = CreateJobInState(database, ObjectId.GenerateNewId(1), FailedState.StateName);
				var failedJob1 = CreateJobInState(database, ObjectId.GenerateNewId(2), FailedState.StateName);
				var failedJob2 = CreateJobInState(database, ObjectId.GenerateNewId(3), FailedState.StateName);


				var jobIds = new List<string>
				{
					failedJob0.Id.ToString(),
					failedJob1.Id.ToString(),
					failedJob2.Id.ToString()
				};
				_persistentJobQueueMonitoringApi.Setup(x => x
						.GetFetchedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.FailedJobs(From, PerPage);

				Assert.Equal(failedJob0.Id.ToString(), resultList[2].Key);
				Assert.Equal(failedJob1.Id.ToString(), resultList[1].Key);
				Assert.Equal(failedJob2.Id.ToString(), resultList[0].Key);
			});
		}

		[Fact, CleanDatabase]
		public void SucceededByDatesCount_ReturnsSuccededJobs_ForLastWeek()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var date = DateTime.UtcNow.Date;
				var counters = new List<CounterDto>();
				var succededCount = 10L;
				for (int i = 0; i < succededCount; i++)
				{
					counters.Add(new CounterDto
					{
						Id = ObjectId.GenerateNewId(),
						// this might fail if we test during date change... seems unlikely
						// TODO, wrap Datetime in a mock friendly wrapper
						Key = $"stats:succeeded:{date:yyyy-MM-dd}",
						Value = 1L
					});
				}

				database.StateData.OfType<CounterDto>().InsertMany(counters);
				database.StateData.OfType<AggregatedCounterDto>().InsertOne(new AggregatedCounterDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = $"stats:succeeded:{date:yyyy-MM-dd}",
					Value = 1L
				});
				var results = monitoringApi.SucceededByDatesCount();

				Assert.Equal(succededCount + 1, results[date]);
				Assert.Equal(8, results.Count);
			});
		}

		[Fact, CleanDatabase]
		public void HourlySucceededJobs_ReturnsSuccededJobs_ForLast24Hours()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var now = DateTime.UtcNow;
				var counters = new List<CounterDto>();
				var succeededCount = 10L;
				for (int i = 0; i < succeededCount; i++)
				{
					counters.Add(new CounterDto
					{
						Id = ObjectId.GenerateNewId(),
						// this might fail if we test during hour change... still unlikely
						// TODO, wrap Datetime in a mock friendly wrapper
						Key = $"stats:succeeded:{now:yyyy-MM-dd-HH}",
						Value = 1L
					});
				}

				database.StateData.OfType<CounterDto>().InsertMany(counters);
				database.StateData.OfType<AggregatedCounterDto>().InsertOne(new AggregatedCounterDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = $"stats:succeeded:{now:yyyy-MM-dd-HH}",
					Value = 1L
				});

				var results = monitoringApi.HourlySucceededJobs();

				Assert.Equal(succeededCount + 1, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
				Assert.Equal(24, results.Count);

			});
		}

		[Fact, CleanDatabase]
		public void FailedByDatesCount_ReturnsFailedJobs_ForLastWeek()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var date = DateTime.UtcNow.Date;
				var counters = new List<CounterDto>();
				var failedCount = 10L;
				for (int i = 0; i < failedCount; i++)
				{
					counters.Add(new CounterDto
					{
						Id = ObjectId.GenerateNewId(),
						// this might fail if we test during date change... seems unlikely
						Key = $"stats:failed:{date:yyyy-MM-dd}",
						Value = 1L
					});
				}

				database.StateData.OfType<CounterDto>().InsertMany(counters);
				database.StateData.OfType<AggregatedCounterDto>().InsertOne(new AggregatedCounterDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = $"stats:failed:{date:yyyy-MM-dd}",
					Value = 1L
				});
				var results = monitoringApi.FailedByDatesCount();

				Assert.Equal(failedCount + 1, results[date]);
				Assert.Equal(8, results.Count);

			});
		}

		[Fact, CleanDatabase]
		public void HourlyFailedJobs_ReturnsFailedJobs_ForLast24Hours()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var now = DateTime.UtcNow;
				var counters = new List<CounterDto>();
				var failedCount = 10L;
				for (int i = 0; i < failedCount; i++)
				{
					counters.Add(new CounterDto
					{
						Id = ObjectId.GenerateNewId(),
						// this might fail if we test during hour change... still unlikely
						// TODO, wrap Datetime in a mock friendly wrapper
						Key = $"stats:failed:{now:yyyy-MM-dd-HH}",
						Value = 1L
					});
				}

				database.StateData.OfType<CounterDto>().InsertMany(counters);
				database.StateData.OfType<AggregatedCounterDto>().InsertOne(new AggregatedCounterDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = $"stats:failed:{now:yyyy-MM-dd-HH}",
					Value = 1L
				});

				var results = monitoringApi.HourlyFailedJobs();

				Assert.Equal(failedCount + 1, results.First(kv => kv.Key.Hour.Equals(now.Hour)).Value);
				Assert.Equal(24, results.Count);

			});
		}
		#endif
	    
		private JobDto CreateJobInState(string stateName, DateTime created = default(DateTime), Action<JobDto> visitor = null)
		{
			var job = Job.FromExpression(() => HangfireTestJobs.SampleMethod("wrong"));

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
				Reason = null,
				Created = DateTime.UtcNow
			};
			foreach (var item in stateData)
			{
				jobState.Data.Add(new KeyValueDto
					{
						Key = item.Key,
						Value = item.Value
					});
			}
			
			var jobDto = new JobDto
			{
				Id = Guid.NewGuid().ToString(),
				Created = created,
				InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
				Arguments = "[\"\\\"Arguments\\\"\"]",
				StateName = stateName
			};
			jobDto.StateHistory.Add(jobState);
			
			visitor?.Invoke(jobDto);
			
			_realm.Write(() =>_realm.Add(jobDto));
			
			var jobQueueDto = new JobQueueDto
			{
				Id = Guid.NewGuid().ToString(),
				Created = DateTime.UtcNow,
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