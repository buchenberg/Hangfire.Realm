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
			throw new NotImplementedException();
		}

		public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
		{
			throw new NotImplementedException();
		}

		public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
		{
			throw new NotImplementedException();
		}

		public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
		{
			throw new NotImplementedException();
		}

		public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
		{
			throw new NotImplementedException();
		}

		public JobList<FailedJobDto> FailedJobs(int @from, int count)
		{
			throw new NotImplementedException();
		}

		public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
		{
			throw new NotImplementedException();
		}

		public long ScheduledCount()
		{
			throw new NotImplementedException();
		}

		public long EnqueuedCount(string queue)
		{
			throw new NotImplementedException();
		}

		public long FetchedCount(string queue)
		{
			throw new NotImplementedException();
		}

		public long FailedCount()
		{
			throw new NotImplementedException();
		}

		public long ProcessingCount()
		{
			throw new NotImplementedException();
		}

		public long SucceededListCount()
		{
			throw new NotImplementedException();
		}

		public long DeletedListCount()
		{
			throw new NotImplementedException();
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
