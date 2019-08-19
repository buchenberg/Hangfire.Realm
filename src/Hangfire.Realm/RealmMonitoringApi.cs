using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Realm.Extensions;
using Hangfire.Realm.Models;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Realm
{
	internal class RealmMonitoringApi : IMonitoringApi
	{
		private readonly IRealmDbContext _realmDbContext;

        public RealmMonitoringApi(RealmJobStorage storage)
		{
			_realmDbContext = storage.GetDbContext();
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
		{
            using (var realm = _realmDbContext.GetRealm())
            {
                var queues = realm.GetQueues();

                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);
                foreach (var queue in queues)
                {
                    var enqueuedJobIds = realm.GetEnqueuedJobIds(queue, 0, 5);
                    var counters = realm.GetEnqueuedAndFetchedCount(queue);
                    var enqueudJobs = realm.GetEnqueuedJobs(enqueuedJobIds);

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Name = queue,
                        Length = counters.enqueuedCount,
                        Fetched = counters.fetchedCount,
                        FirstJobs = enqueudJobs
                    });
                }

                return result;

            }
               
		}

		public IList<Storage.Monitoring.ServerDto> Servers()
		{
            var realm = _realmDbContext.GetRealm();
            
            var servers = realm
            .All<Models.ServerDto>()
            .ToList()
            .Select(s =>
                new Storage.Monitoring.ServerDto
                {
                    Name = s.Id,
                    Heartbeat = s.LastHeartbeat?.DateTime,
                    Queues = s.Queues,
                    StartedAt = s.StartedAt?.DateTime ?? default,
                    WorkersCount = s.WorkerCount
                })
            .ToList();

            return servers;
            
		}

		public JobDetailsDto JobDetails(string jobId)
		{
			var realm = _realmDbContext.GetRealm();
			var job = realm.Find<JobDto>(jobId);

			if (job == null) return null;

			var history = job.StateHistory.Select(x => new StateHistoryDto
				{
					StateName = x.Name,
					CreatedAt = x.Created.DateTime,
					Reason = x.Reason,
					Data = x.Data.ToDictionary(s => s.Key, s => s.Value)
				})
				.ToList();

			return new JobDetailsDto
			{
				CreatedAt = job.Created.DateTime,
				Job = DeserializeJob(job.InvocationData, job.Arguments),
				History = history,
				Properties = job.Parameters.ToDictionary(s => s.Key, s => s.Value)
			};
		}

		public StatisticsDto GetStatistics()
		{
			var realm = _realmDbContext.GetRealm();
			var stats = new StatisticsDto();

			var countByStates = realm
				.All<JobDto>()
				.Where(job => job.StateName == EnqueuedState.StateName || 
				              job.StateName == FailedState.StateName || 
				              job.StateName == ProcessingState.StateName || 
				              job.StateName == ScheduledState.StateName)
				.ToList()
				.GroupBy(job => job.StateName)
				.ToDictionary(group => group.Key, jobs => jobs.Count());


			int GetCountIfExists(string name) => countByStates.ContainsKey(name) ? countByStates[name] : 0;

			stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
			stats.Failed = GetCountIfExists(FailedState.StateName);
			stats.Processing = GetCountIfExists(ProcessingState.StateName);
			stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

			stats.Servers = realm.All<Models.ServerDto>().Count();

			stats.Succeeded = realm.Find<CounterDto>(Constants.StatsSucceded)?.Value ?? 0;
			stats.Deleted = realm.Find<CounterDto>(Constants.StatsDeleted)?.Value ?? 0;

			stats.Recurring = realm
				.All<SetDto>()
				.Count(s => s.Key == Constants.RecurringJobs);


			stats.Queues = realm.GetQueues().Count;

			return stats;
		}

		public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
		{
			var realm = _realmDbContext.GetRealm();
			var jobIds = realm.GetEnqueuedJobIds(queue, from, perPage);
			var jobs = realm.GetEnqueuedJobs(jobIds);
			return jobs;
		}

		public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
		{
			var realm = _realmDbContext.GetRealm();
			var jobIds = realm.GetFetchedJobIds(queue, from, perPage);
			var jobs = realm.GetFetchedJobs(jobIds);
			return jobs;
		}

		public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
		{
			var realm = _realmDbContext.GetRealm();
			var jobs = realm.GetJobsByStateName(ProcessingState.StateName, from, count);
			
			return GetJobs(jobs, ProcessingState.StateName, (job, stateData, stateReason) => new ProcessingJobDto
			{
				Job = job,
				ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
				StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"])
			});
		}

		public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
		{
			var realm = _realmDbContext.GetRealm();
			var jobs = realm.GetJobsByStateName(ScheduledState.StateName, from, count);
			
			return GetJobs(jobs, ScheduledState.StateName, (job, stateData, stateReason) => new ScheduledJobDto
			{
				Job = job,
				EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
				ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
			});
		}

		public JobList<SucceededJobDto> SucceededJobs(int from, int count)
		{
			var realm = _realmDbContext.GetRealm();
			var jobs = realm.GetJobsByStateName(SucceededState.StateName, from, count);
			
			return GetJobs(jobs, SucceededState.StateName, (job, stateData, stateReason) => new SucceededJobDto
			{
				Job = job,
				Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
				TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
					? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
					: null,
				SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
			});
		}

		public JobList<FailedJobDto> FailedJobs(int from, int count)
		{
			var realm = _realmDbContext.GetRealm();
			var jobs = realm.GetJobsByStateName(FailedState.StateName, from, count);
			
			return GetJobs(jobs, FailedState.StateName, (job, stateData, stateReason) => new FailedJobDto
			{
				Job = job,
				Reason = stateReason,
				ExceptionDetails = stateData["ExceptionDetails"],
				ExceptionMessage = stateData["ExceptionMessage"],
				ExceptionType = stateData["ExceptionType"],
				FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
			});
		}

		public JobList<DeletedJobDto> DeletedJobs(int from, int count)
		{
			var realm = _realmDbContext.GetRealm();
			var jobs = realm.GetJobsByStateName(DeletedState.StateName, from, count);
			
			return GetJobs(jobs, DeletedState.StateName, (job, stateData, stateReason) => new DeletedJobDto
			{
				Job = job,
				DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
			});
		}

		public long ScheduledCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetJobCountByStateName(ScheduledState.StateName);
		}

		public long EnqueuedCount(string queue)
		{
			var realm = _realmDbContext.GetRealm();
			return realm.All<JobQueueDto>().Count(j => j.Queue == queue && j.FetchedAt == null);
		}

		public long FetchedCount(string queue)
		{
			var realm = _realmDbContext.GetRealm();
			return realm.All<JobQueueDto>().Count(j => j.Queue == queue && j.FetchedAt != null);
		}

		public long FailedCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetJobCountByStateName(FailedState.StateName);
		}

		public long ProcessingCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetJobCountByStateName(ProcessingState.StateName);
		}

		public long SucceededListCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetJobCountByStateName(SucceededState.StateName);
		}

		public long DeletedListCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetJobCountByStateName(DeletedState.StateName);
		}

		public IDictionary<DateTime, long> SucceededByDatesCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetTimelineStats(SucceededState.StateName.ToLower());
		}

		public IDictionary<DateTime, long> FailedByDatesCount()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetTimelineStats(FailedState.StateName.ToLower());
		}

		public IDictionary<DateTime, long> HourlySucceededJobs()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetHourlyTimelineStats(SucceededState.StateName.ToLower());
		}

		public IDictionary<DateTime, long> HourlyFailedJobs()
		{
			var realm = _realmDbContext.GetRealm();
			return realm.GetHourlyTimelineStats(FailedState.StateName.ToLower());
		}

		private static JobList<T> GetJobs<T>(IList<JobDto> jobs, string stateName, Func<Common.Job, IDictionary<string, string>, string, T> createDto)
		{
			if (jobs == null)
			{
				throw new ArgumentNullException(nameof(jobs));
			}
			
			var joinedJobs = jobs
				.Select(job =>
				{
					var state = job
						.StateHistory
						.FirstOrDefault(s => s.Name == stateName);

					return new
					{
						job.Id,
						job.InvocationData,
						job.Arguments,
						job.Created,
						job.ExpireAt,
						FetchedAt = (DateTimeOffset?)null,
						job.StateName,
						StateReason = state?.Reason,
						StateData = state?.Data
					};
				})
				.ToList();
			
			var result = new List<KeyValuePair<string, T>>(jobs.Count);
			foreach (var joinedJob in joinedJobs)
			{
				var stateData = joinedJob.StateData.ToDictionary(s => s.Key, s => s.Value);
				var job = DeserializeJob(joinedJob.InvocationData, joinedJob.Arguments);
				result.Add(new KeyValuePair<string, T>(joinedJob.Id, createDto(job, stateData, joinedJob.StateReason)));
			}
			
			return new JobList<T>(result);
		}
		
		private static Job DeserializeJob(string invocationData, string arguments)
		{
			var data = InvocationData.DeserializePayload(invocationData);
			if (!string.IsNullOrEmpty(arguments))
			{
				data.Arguments = arguments;
			}

			try
			{
				return data.DeserializeJob();
			}
			catch (JobLoadException)
			{
				return null;
			}
		}
	}
}
