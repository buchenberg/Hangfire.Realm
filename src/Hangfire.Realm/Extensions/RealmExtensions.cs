using System.Collections.Generic;
using System.Linq;
using Hangfire.Realm.RealmObjects;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Realm.Extensions
{
	internal static class RealmExtensions
    {
		public static IList<string> GetEnqueuedJobIds(this Realms.Realm realm, string queue, int from, int perPage)
		{
			return realm
				.All<JobQueue>()
				.Where(q => q.Queue == queue && q.FetchedAt == null)
				.Skip(from)
				.Take(perPage)
				.Select(q => q.JobId)
				.Where(jobQueueId => realm.All<Job>().Any(j => j.Id == jobQueueId && j.StateHistory.Length > 0))
				.ToList();
		}

		public static (int enqueuedCount, int fetchedCount) GetEnqueuedAndFetchedCount(this Realms.Realm realm, string queue)
		{
			var enqueuedCount = realm.All<JobQueue>().Count(q => q.Queue == queue && q.FetchedAt == null);

			var fetchedCount = realm.All<JobQueue>().Count(q => q.Queue == queue && q.FetchedAt != null);
			
			return (enqueuedCount, fetchedCount);
		}

	    public static JobList<EnqueuedJobDto> GetEnqueuedJobs(this Realms.Realm realm, IEnumerable<string> jobIds)
	    {
		    var jobs = realm.All<Job>().Where(j => jobIds.Contains(j.Id)).ToList();
		    var enqueuedJobs = realm.All<JobQueue>().Where(q => jobs.Select(j => j.Id).Contains(q.JobId) && q.FetchedAt == null)
			    .ToList();
		    var jobsFiltered = enqueuedJobs
			    .Select(jq => jobs.FirstOrDefault(job => job.Id == jq.JobId));
		    var joinedJobs = jobsFiltered
			    .Where(job => job != null)
			    .Select(job =>
			    {
				    var state = job.StateHistory.LastOrDefault();
				    return new JobDetailedDto
				    {
					    Id = job.Id,
					    InvocationData = job.InvocationData,
					    Arguments = job.Arguments,
					    CreatedAt = job.CreatedAt,
					    ExpireAt = job.ExpireAt,
					    FetchedAt = null,
					    StateName = job.StateName,
					    StateReason = state?.Reason,
					    StateData = state?.Data
				    };
			    })
			    .ToList();
		    /*
		     var jobObjectIds = jobIds.Select(ObjectId.Parse);
            var jobs = connection.Job
                .Find(Builders<JobDto>.Filter.In(_ => _.Id, jobObjectIds))
                .ToList();

            var filterBuilder = Builders<JobQueueDto>.Filter;
            var enqueuedJobs = connection.JobQueue
                .Find(filterBuilder.In(_ => _.JobId, jobs.Select(job => job.Id)) &
                      (filterBuilder.Not(filterBuilder.Exists(_ => _.FetchedAt)) | filterBuilder.Eq(_ => _.FetchedAt, null)))
                .ToList();

            var jobsFiltered = enqueuedJobs
                .Select(jq => jobs.FirstOrDefault(job => job.Id == jq.JobId));

            var joinedJobs = jobsFiltered
                .Where(job => job != null)
                .Select(job =>
                {
                    var state = job.StateHistory.LastOrDefault();
                    return new JobDetailedDto
                    {
                        Id = job.Id,
                        InvocationData = job.InvocationData,
                        Arguments = job.Arguments,
                        CreatedAt = job.CreatedAt,
                        ExpireAt = job.ExpireAt,
                        FetchedAt = null,
                        StateName = job.StateName,
                        StateReason = state?.Reason,
                        StateData = state?.Data
                    };
                })
                .ToList();

            return DeserializeJobs(
                joinedJobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    EnqueuedAt = sqlJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
		     */
	    }
    }
}
