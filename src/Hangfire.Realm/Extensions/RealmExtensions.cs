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
				.All<JobQueueDto>()
				.Where(q => q.Queue == queue && q.FetchedAt == null)
				.Skip(from)
				.Take(perPage)
				.Select(q => q.JobId)
				.Where(jobQueueId => realm.All<JobDto>().Any(j => j.Id == jobQueueId && j.StateHistory.Length > 0))
				.ToList();
		}

		public static (int enqueuedCount, int fetchedCount) GetEnqueuedAndFetchedCount(this Realms.Realm realm, string queue)
		{
			var enqueuedCount = realm.All<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt == null);

			var fetchedCount = realm.All<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt != null);
			
			return (enqueuedCount, fetchedCount);
		}
	   
	    public static JobList<EnqueuedJobDto> GetEnqueuedJobs(this Realms.Realm realm, IEnumerable<string> jobIds)
	    {
		    var jobs = realm.All<JobDto>().Where(j => jobIds.Contains(j.Id)).ToList();
		    
		    var enqueuedJobs = realm.All<JobQueueDto>().Where(q => jobs.Select(j => j.Id).Contains(q.JobId) && q.FetchedAt == null)
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

	    private static DateTime? GetEnqueudAt(JobDto job)
	    {
		    var state = job.StateHistory.LastOrDefault();
		    return job.StateName == EnqueuedState.StateName
			    ? JobHelper.DeserializeNullableDateTime(state.Data.First(d => d.Key == "EnqueuedAt").Value)
			    : null;
	    }
    }
}
