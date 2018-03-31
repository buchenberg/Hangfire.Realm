using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Realm.Extensions;
using Hangfire.Realm.RealmObjects;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Realm
{
	internal class RealmMonitoringApi : IMonitoringApi
	{
		private readonly Realms.Realm _realm;

		public RealmMonitoringApi(RealmJobStorageOptions options)
		{
			_realm = Realms.Realm.GetInstance(options.RealmConfiguration);
		}

		public IList<QueueWithTopEnqueuedJobsDto> Queues()
		{

			var queues = _realm
				.All<JobQueueRealmObject>()
				.Select(q => q.Queue)
				.Distinct()
				.ToList();

			var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);
			foreach (var queue in queues)
			{
				var enqueuedJobIds = _realm.GetEnqueuedJobIds(queue, 0, 5);
				var counters = _realm.GetEnqueuedAndFetchedCount(queue);
				var enqueudJobs = _realm.GetEnqueuedJobs(enqueuedJobIds);

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



		public IList<ServerDto> Servers()
		{
			var servers = _realm
				.All<ServerRealmObject>()
				.ToList()
				.Select(s =>
					new ServerDto
					{
						Name = s.Id,
						Heartbeat = s.LastHeartbeat?.DateTime,
						Queues = s.Queues,
						StartedAt = s.StartedAt.Value.DateTime,
						WorkersCount = s.WorkerCount
					})
				.ToList();

			return servers;
		}

		public JobDetailsDto JobDetails(string jobId)
		{
			var job = _realm
				.All<JobRealmObject>()
				.FirstOrDefault(j => j.Id == jobId);

			if (job == null) return null;

			var history = job.StateHistory.Select(x => new StateHistoryDto
				{
					StateName = x.Name,
					CreatedAt = x.CreatedAt.DateTime,
					Reason = x.Reason,
					Data = x.Data.ToDictionary(s => s.Key, s => s.Value)
				})
				.Reverse()
				.ToList();

			return new JobDetailsDto
			{
				CreatedAt = job.CreatedAt.DateTime,
				Job = DeserializeJob(job.InvocationData, job.Arguments),
				History = history,
				Properties = job.Parameters.ToDictionary(s => s.Key, s => s.Value)
			};
		}

		private static readonly string[] StatisticsStateNames =
		{
			EnqueuedState.StateName,
			FailedState.StateName,
			ProcessingState.StateName,
			ScheduledState.StateName
		};

		public StatisticsDto GetStatistics()
		{
			var stats = new StatisticsDto();


			var countByStates = _realm
				.All<JobRealmObject>()
				.Where(job => StatisticsStateNames.Contains(job.StateName))
				.GroupBy(job => job.StateName)
				.ToDictionary(group => group.Key, jobs => jobs.Count());


			int GetCountIfExists(string name) => countByStates.ContainsKey(name) ? countByStates[name] : 0;

			stats.Enqueued = GetCountIfExists(EnqueuedState.StateName);
			stats.Failed = GetCountIfExists(FailedState.StateName);
			stats.Processing = GetCountIfExists(ProcessingState.StateName);
			stats.Scheduled = GetCountIfExists(ScheduledState.StateName);

			stats.Servers = _realm.All<ServerRealmObject>().Count();

			stats.Succeeded = _realm
				.All<CounterRealmObject>()
				.Where(c => c.Key == Constants.StatsSucceded)
				.Sum(c => (long) c.Value);

			stats.Deleted = _realm
				.All<CounterRealmObject>()
				.Where(c => c.Key == Constants.StatsDeleted)
				.Sum(c => (long) c.Value);

			stats.Recurring = _realm
				.All<SetRealmObject>()
				.Count(s => s.Key == Constants.RecurringJobs);


			stats.Queues = _realm.GetQueues().Count;

			return stats;
		}

		public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
		{
			var jobIds = _realm.GetEnqueuedJobIds(queue, from, perPage);
			var jobs = _realm.GetEnqueuedJobs(jobIds);
			return jobs;
		}

		public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
		{
			var jobIds = _realm.GetFetchedJobIds(queue, from, perPage);
			var jobs = _realm.GetFetchedJobs(jobIds);
			return jobs;
		}

		public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
		{
			var jobs = _realm.GetJobsByStateName(ProcessingState.StateName, from, count);
			
			return GetJobs(jobs, (job, stateData, stateReason) => new ProcessingJobDto
			{
				Job = job,
				ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
				StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"])
			});
		}

		public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
		{
			var jobs = _realm.GetJobsByStateName(ScheduledState.StateName, from, count);
			
			return GetJobs(jobs, (job, stateData, stateReason) => new ScheduledJobDto
			{
				Job = job,
				EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
				ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
			});
		}

		public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
		{
			var jobs = _realm.GetJobsByStateName(SucceededState.StateName, from, count);
			
			return GetJobs(jobs, (job, stateData, stateReason) => new SucceededJobDto
			{
				Job = job,
				Result = stateData.ContainsKey("Result") ? stateData["Result"] : null,
				TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
					? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
					: null,
				SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
			});
		}

		public JobList<FailedJobDto> FailedJobs(int @from, int count)
		{
			var jobs = _realm.GetJobsByStateName(FailedState.StateName, from, count);
			
			return GetJobs(jobs, (job, stateData, stateReason) => new FailedJobDto
			{
				Job = job,
				Reason = stateReason,
				ExceptionDetails = stateData["ExceptionDetails"],
				ExceptionMessage = stateData["ExceptionMessage"],
				ExceptionType = stateData["ExceptionType"],
				FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
			});
		}

		public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
		{
			var jobs = _realm.GetJobsByStateName(DeletedState.StateName, from, count);
			
			return GetJobs(jobs, (job, stateData, stateReason) => new DeletedJobDto
			{
				Job = job,
				DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
			});
		}

		public long ScheduledCount()
		{
			return _realm.GetJobCountByStateName(ScheduledState.StateName);
		}

		public long EnqueuedCount(string queue)
		{
			return _realm.All<JobQueueRealmObject>().Count(j => j.Queue == queue && j.FetchedAt == null);
		}

		public long FetchedCount(string queue)
		{
			return _realm.All<JobQueueRealmObject>().Count(j => j.Queue == queue && j.FetchedAt != null);
		}

		public long FailedCount()
		{
			return _realm.GetJobCountByStateName(FailedState.StateName);
		}

		public long ProcessingCount()
		{
			return _realm.GetJobCountByStateName(ProcessingState.StateName);
		}

		public long SucceededListCount()
		{
			return _realm.GetJobCountByStateName(SucceededState.StateName);
		}

		public long DeletedListCount()
		{
			return _realm.GetJobCountByStateName(DeletedState.StateName);
		}

		public IDictionary<DateTime, long> SucceededByDatesCount()
		{
			throw new NotImplementedException();
		}

		public IDictionary<DateTime, long> FailedByDatesCount()
		{
			throw new NotImplementedException();
		}

		public IDictionary<DateTime, long> HourlySucceededJobs()
		{
			throw new NotImplementedException();
		}

		public IDictionary<DateTime, long> HourlyFailedJobs()
		{
			throw new NotImplementedException();
		}

		private static JobList<T> GetJobs<T>(IList<JobRealmObject> jobs, Func<Job, IDictionary<string, string>, string, T> createDto)
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
						.FirstOrDefault(s => s.Name == ProcessingState.StateName);

					return new
					{
						Id = job.Id,
						InvocationData = job.InvocationData,
						Arguments = job.Arguments,
						CreatedAt = job.CreatedAt,
						ExpireAt = job.ExpireAt,
						FetchedAt = (DateTimeOffset?)null,
						StateName = job.StateName,
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
			var data = JobHelper.FromJson<InvocationData>(invocationData);
			data.Arguments = arguments;

			try
			{
				return data.Deserialize();
			}
			catch (JobLoadException)
			{
				return null;
			}
		}
	}
}
