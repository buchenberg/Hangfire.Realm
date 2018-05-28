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
			;
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
	    
		#if false
		[Fact, CleanDatabase]
		

		[Fact, CleanDatabase]
		

		

		[Fact, CleanDatabase]
		public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), FetchedStateName);

				var jobIds = new List<string> { fetchedJob.Id.ToString() };
				_persistentJobQueueMonitoringApi.Setup(x => x
					.GetEnqueuedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

				Assert.Empty(resultList);
			});
		}

		[Fact, CleanDatabase]
		public void EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);
				var unfetchedJob2 = CreateJobInState(database, ObjectId.GenerateNewId(2), EnqueuedState.StateName);
				var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(3), FetchedStateName);

				var jobIds = new List<string>
				{
					unfetchedJob.Id.ToString(),
					unfetchedJob2.Id.ToString(),
					fetchedJob.Id.ToString()
				};
				_persistentJobQueueMonitoringApi.Setup(x => x
					.GetEnqueuedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

				Assert.Equal(2, resultList.Count);
			});
		}

		[Fact, CleanDatabase]
		public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var jobIds = new List<string>();

				_persistentJobQueueMonitoringApi.Setup(x => x
					.GetFetchedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

				Assert.Empty(resultList);
			});
		}

		[Fact, CleanDatabase]
		public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), FetchedStateName);

				var jobIds = new List<string> { fetchedJob.Id.ToString() };
				_persistentJobQueueMonitoringApi.Setup(x => x
					.GetFetchedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

				Assert.Single(resultList);
			});
		}

		[Fact, CleanDatabase]
		public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), EnqueuedState.StateName);

				var jobIds = new List<string> { unfetchedJob.Id.ToString() };
				_persistentJobQueueMonitoringApi.Setup(x => x
					.GetFetchedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

				Assert.Empty(resultList);
			});
		}

		[Fact, CleanDatabase]
		public void FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var fetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(1), FetchedStateName);
				var fetchedJob2 = CreateJobInState(database, ObjectId.GenerateNewId(2), FetchedStateName);
				var unfetchedJob = CreateJobInState(database, ObjectId.GenerateNewId(3), EnqueuedState.StateName);

				var jobIds = new List<string>
				{
					fetchedJob.Id.ToString(),
					fetchedJob2.Id.ToString(),
					unfetchedJob.Id.ToString()
				};
				_persistentJobQueueMonitoringApi.Setup(x => x
					.GetFetchedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

				Assert.Equal(2, resultList.Count);
			});
		}

		[Fact, CleanDatabase]
		public void ProcessingJobs_ReturnsProcessingJobsOnly_WhenMultipleJobsExistsInProcessingSucceededAndEnqueuedState()
		{
			UseMonitoringApi((database, monitoringApi) =>
			{
				var processingJob = CreateJobInState(database, ObjectId.GenerateNewId(1), ProcessingState.StateName);

				var succeededJob = CreateJobInState(database, ObjectId.GenerateNewId(2), SucceededState.StateName, jobDto =>
				{
					var processingState = new StateDto()
					{
						Name = ProcessingState.StateName,
						Reason = null,
						CreatedAt = DateTime.UtcNow,
						Data = new Dictionary<string, string>
						{
							["ServerId"] = Guid.NewGuid().ToString(),
							["StartedAt"] =
							JobHelper.SerializeDateTime(DateTime.UtcNow.Subtract(TimeSpan.FromMilliseconds(500)))
						}
					};
					var succeededState = jobDto.StateHistory[0];
					jobDto.StateHistory = new[] { processingState, succeededState };
					return jobDto;
				});

				var enqueuedJob = CreateJobInState(database, ObjectId.GenerateNewId(3), EnqueuedState.StateName);

				var jobIds = new List<string>
				{
					processingJob.Id.ToString(),
					succeededJob.Id.ToString(),
					enqueuedJob.Id.ToString()
				};
				_persistentJobQueueMonitoringApi.Setup(x => x
						.GetFetchedJobIds(DefaultQueue, From, PerPage))
					.Returns(jobIds);

				var resultList = monitoringApi.ProcessingJobs(From, PerPage);

				Assert.Single(resultList);
			});
		}

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
		private JobDto CreateJobInState(string stateName, DateTime created, Func<JobDto, JobDto> visitor = null)
		{
			var job = Job.FromExpression(() => HangfireTestJobs.SampleMethod("wrong"));

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
			
			if (visitor != null)
			{
				jobDto = visitor(jobDto);
			}
			_realm.Write(() =>
			{
				_realm.Add(jobDto);
				
			});
			
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