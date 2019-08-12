using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Hangfire.Common;
using Hangfire.Realm.Models;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Realms;

namespace Hangfire.Realm.Extensions
{
	internal static class RealmExtensions
    {
	    public static IList<string> GetEnqueuedJobIds(this Realms.Realm realm, string queue, int from, int perPage)
	    {
		    var jobIds = new List<string>();
		    var count = 0;
		    foreach (var queuedJob in realm
			    .All<JobQueueDto>()
			    .OrderByDescending(q => q.Created)
			    .Where(q => q.Queue == queue && q.FetchedAt == null))
		    {
			    if (from > count++) continue;
			    if (jobIds.Count >= perPage) return jobIds;
			    
			    var job = realm.Find<JobDto>(queuedJob.JobId);
			    if (job != null && job.StateHistory.Count > 0)
			    {
				    jobIds.Add(queuedJob.JobId);
			    }
		    }

		    return jobIds;
	    }

	    public static (int enqueuedCount, int fetchedCount) GetEnqueuedAndFetchedCount(this Realms.Realm realm, string queue)
		{
			var enqueuedCount = realm.All<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt == null);

			var fetchedCount = realm.All<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt != null);
			
			return (enqueuedCount, fetchedCount);
		}
	   
	    public static JobList<EnqueuedJobDto> GetEnqueuedJobs(this Realms.Realm realm, IList<string> jobIds)
	    {
		    var jobs = FindJobs(realm, jobIds);

		    var enqueuedJobs = FindQueuedJobs(realm, jobIds, enqueued: true);
			    
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
			    .All<JobQueueDto>()
				.ToList()
			    .Select(j => j.Queue)
			    .ToArray();
	    }

	    public static IList<string> GetFetchedJobIds(this Realms.Realm realm, string queue, int from, int perPage)
	    {
		    var fetchedJobIds = new List<string>();
		    var count = 0;
		    foreach (var jobQueueDto in realm
			    .All<JobQueueDto>()
			    .OrderByDescending(q => q.Created)
			    .Where(j => j.Queue == queue && j.FetchedAt != null))
		    {
			    if (from > count++) continue;
			    if (fetchedJobIds.Count >= perPage) return fetchedJobIds;
			    
			    var job = realm.Find<JobDto>(jobQueueDto.JobId);
			    if (job != null)
			    {
				    fetchedJobIds.Add(jobQueueDto.JobId);
			    }
		    }
		    return fetchedJobIds;
	    }

	    public static JobList<FetchedJobDto> GetFetchedJobs(this Realms.Realm realm, ICollection<string> jobIds)
	    {
		    var jobs = FindJobs(realm, jobIds);
		    var foundJobIds = jobs.Select(j => j.Id).ToList();

		    var jobIdToJobQueueMap = FindQueuedJobs(realm, foundJobIds, enqueued: false)
			    .ToDictionary(kv => kv.JobId, kv => kv); 
		  
		    var jobsFiltered = jobs.Where(job => jobIdToJobQueueMap.ContainsKey(job.Id));
		    
		    var joinedJobs = jobsFiltered
			    .Select(job =>
			    {
				    var state = job.StateHistory.FirstOrDefault(s => s.Name == job.StateName);
				    return new 
				    {
					    job.Id,
                        job.InvocationData,
					    job.Arguments,
					    CreatedAt = job.Created,
					    job.ExpireAt,
					    FetchedAt = (DateTimeOffset?)null,
					    job.StateName,
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

	    public static IList<JobDto> GetJobsByStateName(this Realms.Realm realm, string stateName, int from, int perPage)
	    {
		    var jobs = new List<JobDto>();
		    var count = 0;
		    
		    foreach (var job in realm
			    .All<JobDto>()
			    .Where(j => j.StateName == stateName)
			    .OrderByDescending(j => j.Created))
		    {
			    if (from > count++) continue;
			    if (jobs.Count >= perPage) return jobs;
			    
			    jobs.Add(job);
		    }
		    return jobs;    
	    }

	    public static long GetJobCountByStateName(this Realms.Realm realm, string stateName)
	    {
		    var count = realm.All<JobDto>().Count(j => j.StateName == stateName);
		    return count;
	    }
	    
	    public static Dictionary<DateTime, long> GetTimelineStats(this Realms.Realm realm, string type)
	    {
		    var endDate = DateTime.UtcNow.Date;
		    var startDate = endDate.AddDays(-7);
		    var dates = new List<DateTime>();

		    while (startDate <= endDate)
		    {
			    dates.Add(endDate);
			    endDate = endDate.AddDays(-1);
		    }

		    var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
		    var names = stringDates.Select(x => $"stats:{type}:{x}").ToList();

		    return realm.CreateTimeLineStats(names, dates);
	    }
	    
	    public static Dictionary<DateTime, long> GetHourlyTimelineStats(this Realms.Realm realm, string type)
	    {
		    var endDate = DateTime.UtcNow;
		    var dates = new List<DateTime>();
		    for (var i = 0; i < 24; i++)
		    {
			    dates.Add(endDate);
			    endDate = endDate.AddHours(-1);
		    }

		    var names = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}").ToList();

		    return realm.CreateTimeLineStats(names, dates);
	    }
		
	    private static IList<JobDto> FindJobs(Realms.Realm realm, ICollection<string> jobIds)
	    {
		    if (jobIds.Count == 0)
		    {
			    return Enumerable.Empty<JobDto>().ToList();
		    }
		    
		    var allJobs = realm.All<JobDto>();
		    
		    var param = Expression.Parameter(typeof(JobDto), "p");

		    // ReSharper disable once AssignNullToNotNullAttribute
		    var jobIdExp = Expression.Property(param, typeof(JobDto).GetProperty(nameof(JobDto.Id)));

		    var filterExp = CreateOrElsExpression(jobIdExp, jobIds);

		    return Query(allJobs, filterExp, param);
	    }
	    
	    private static IList<JobQueueDto> FindQueuedJobs(Realms.Realm realm, ICollection<string> jobIds, bool enqueued)
	    {
		    if (jobIds.Count == 0)
		    {
			    return Enumerable.Empty<JobQueueDto>().ToList();
		    }
		    
		    var allJobs = realm.All<JobQueueDto>();
		    
		    var param = Expression.Parameter(typeof(JobQueueDto), "p");

		    // ReSharper disable AssignNullToNotNullAttribute
		    var fetchedAtExp =
			    Expression.Property(param, typeof(JobQueueDto).GetProperty(nameof(JobQueueDto.FetchedAt)));
		    var jobIdExp = Expression.Property(param, typeof(JobQueueDto).GetProperty(nameof(JobQueueDto.JobId)));
		    // ReSharper enable AssignNullToNotNullAttribute

		    var filterExp = CreateOrElsExpression(jobIdExp, jobIds);

		    if (filterExp == null)
		    {
			    return Enumerable.Empty<JobQueueDto>().ToList();
		    }

		    var equalExp = enqueued ? 
			    Expression.Equal(fetchedAtExp, Expression.Constant(null)) : 
			    Expression.NotEqual(fetchedAtExp, Expression.Constant(null));
		    
		    filterExp = Expression.AndAlso(equalExp, filterExp);

		    return Query(allJobs, filterExp, param);
	    }

	    private static Expression CreateOrElsExpression(MemberExpression memberExpression, IEnumerable<string> jobIds)
	    {
		    Expression orElse = null;
		    foreach (var jobId in jobIds)
		    {
			    var equal = Expression.Equal(memberExpression, Expression.Constant(jobId));
			    orElse = orElse == null ? equal : Expression.OrElse(orElse, equal);
		    }

		    return orElse;
	    }
	    private static IList<T> Query<T>(IQueryable<T> queryable, Expression filter, ParameterExpression param)
	    {
		    if (filter == null)
		    {
			    return Enumerable.Empty<T>().ToList();
		    }
		    
		    var whereCallExpression = Expression.Call(
			    typeof(Queryable),
			    nameof(Queryable.Where),
			    new[] {typeof(T)},
			    queryable.Expression,
			    Expression.Lambda<Func<T, bool>>(filter, param));

		    
		    // Create an executable query from the expression tree.
		    return queryable.Provider.CreateQuery<T>(whereCallExpression).ToList();
	    }
	    
	    private static Dictionary<DateTime, long> CreateTimeLineStats(this Realms.Realm realm,
		    ICollection<string> keys, IList<DateTime> dates)
	    {
		    var valuesMap = new Dictionary<string, long>();
		    foreach (var key in keys)
		    {
			    valuesMap[key] = 0;
			    var counter = realm.Find<CounterDto>(key);
			    if (counter != null)
			    {
				    valuesMap[key] = counter.Value;
			    }
		    }
		    
		    var result = new Dictionary<DateTime, long>();
		    for (var i = 0; i < dates.Count; i++)
		    {
			    var value = valuesMap[valuesMap.Keys.ElementAt(i)];
			    result.Add(dates[i], value);
		    }

		    return result;
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

	    private static DateTime? GetEnqueudAt(JobDto jobDto)
	    {
		    var state = jobDto.StateHistory.LastOrDefault();
		    return jobDto.StateName == EnqueuedState.StateName
			    ? JobHelper.DeserializeNullableDateTime(state.Data.First(d => d.Key == "EnqueuedAt").Value)
			    : null;
	    }
    }
}
