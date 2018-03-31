using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Realm.RealmObjects;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Realm.Extensions
{
	internal static class RealmExtensions
    {
		public static IList<string> GetEnqueuedJobIds(this Realms.Realm realm, string queue, int from, int perPage)
		{
			return realm
				.All<JobQueueRealmObject>()
				.Where(q => q.Queue == queue && q.FetchedAt == null)
				.Skip(from)
				.Take(perPage)
				.Select(q => q.JobId)
				.Where(jobQueueId => realm.All<JobRealmObject>().Any(j => j.Id == jobQueueId && j.StateHistory.Length > 0))
				.ToList();
		}

		public static (int enqueuedCount, int fetchedCount) GetEnqueuedAndFetchedCount(this Realms.Realm realm, string queue)
		{
			var enqueuedCount = realm.All<JobQueueRealmObject>().Count(q => q.Queue == queue && q.FetchedAt == null);

			var fetchedCount = realm.All<JobQueueRealmObject>().Count(q => q.Queue == queue && q.FetchedAt != null);
			
			return (enqueuedCount, fetchedCount);
		}
	   
	    public static JobList<EnqueuedJobDto> GetEnqueuedJobs(this Realms.Realm realm, IEnumerable<string> jobIds)
	    {
		    var jobs = realm.All<JobRealmObject>().Where(j => jobIds.Contains(j.Id)).ToList();
		    
		    var enqueuedJobs = realm.All<JobQueueRealmObject>().Where(q => jobs.Select(j => j.Id).Contains(q.JobId) && q.FetchedAt == null)
			    .ToList();
		    
		    var jobsFiltered = enqueuedJobs
			    .Select(jq => jobs.FirstOrDefault(job => job.Id == jq.JobId));
		    
		    var result = jobsFiltered
			    .Where(job => job != null)
			    .Select(job => new KeyValuePair<string, EnqueuedJobDto>(
				    job.Id,
				    new EnqueuedJobDto
				    {
					    EnqueuedAt = GetEnqueudAt(job),
					    InEnqueuedState = job.StateName == EnqueuedState.StateName,
					    Job = DeserializeJob(job.InvocationData, job.Arguments),
					    State = job.StateName
				    }));
		    
		    return new JobList<EnqueuedJobDto>(result);
	    }

	    public static IList<string> GetQueues(this Realms.Realm realm)
	    {
		    return realm
			    .All<JobQueueRealmObject>()
			    .Select(j => j.Queue)
			    .ToArray();
	    }

	    public static IList<string> GetFetchedJobIds(this Realms.Realm realm, string queue, int from, int perPage)
	    {
		    var fetchedJobIds = realm
			    .All<JobQueueRealmObject>()
			    .Where(j => j.Queue == queue && j.FetchedAt != null)
			    .Skip(from)
			    .Take(perPage)
			    .Select(j => j.JobId)
			    .Where(jobId => realm.All<JobRealmObject>().Any(_ => _.Id == jobId))
			    .ToList();

		    return fetchedJobIds;
	    }
	    public static JobList<FetchedJobDto> GetFetchedJobs(this Realms.Realm realm, IEnumerable<string> jobIds)
	    {
		    var jobs = realm.All<JobRealmObject>().Where(j => jobIds.Contains(j.Id)).ToList();

		    var jobIdToJobQueueMap = realm
			    .All<JobQueueRealmObject>()
			    .Where(q => jobs.Select(j => j.Id).Contains(q.JobId) && q.FetchedAt != null)
			    .ToDictionary(kv => kv.JobId, kv => kv);
			    
		    
		    var jobsFiltered = jobs.Where(job => jobIdToJobQueueMap.ContainsKey(job.Id));
		    
		    var joinedJobs = jobsFiltered
			    .Select(job =>
			    {
				    var state = job.StateHistory.FirstOrDefault(s => s.Name == job.StateName);
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
		    
		    var result = new List<KeyValuePair<string, FetchedJobDto>>(joinedJobs.Count);

		    foreach (var job in joinedJobs)
		    {
			    result.Add(new KeyValuePair<string, FetchedJobDto>(
				    job.Id,
				    new FetchedJobDto
				    {
					    Job = DeserializeJob(job.InvocationData, job.Arguments),
					    State = job.StateName,
					    FetchedAt = job.FetchedAt?.DateTime
				    }));
		    }

		    return new JobList<FetchedJobDto>(result);
	    }

	    public static IList<JobRealmObject> GetJobsByStateName(this Realms.Realm realm, string stateName, int from, int count)
	    {
		    return realm
			    .All<JobRealmObject>()
			    .Where(j => j.StateName == ProcessingState.StateName)
			    .OrderByDescending(j => j.CreatedAt)
			    .Skip(from)
			    .Take(count)
			    .ToList();	    
	    }

	    public static long GetJobCountByStateName(this Realms.Realm realm, string stateName)
	    {
		    var count = realm.All<JobRealmObject>().Count(j => j.StateName == stateName);
		    return count;
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

	    private static DateTime? GetEnqueudAt(JobRealmObject job)
	    {
		    var state = job.StateHistory.LastOrDefault();
		    return job.StateName == EnqueuedState.StateName
			    ? JobHelper.DeserializeNullableDateTime(state.Data.First(d => d.Key == "EnqueuedAt").Value)
			    : null;
	    }
    }
}
