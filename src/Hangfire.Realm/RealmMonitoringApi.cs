using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Hangfire.Realm.Extensions;
using Hangfire.Realm.RealmObjects;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Realm
{
	internal class RealmMonitoringApi : IMonitoringApi
    {
		private readonly Realms.Realm _realm = Realms.Realm.GetInstance();
	    public IList<QueueWithTopEnqueuedJobsDto> Queues()
	    {

		    var queues = _realm
			    .All<JobQueue>()
			    .Select(q => q.Queue)
			    .Distinct()
			    .ToList();

		    var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);
		    foreach (var queue in queues)
		    {
			    var enqueuedJobIds = _realm.GetEnqueuedJobIds(queue, 0, 5);
			    var counters = _realm.GetEnqueuedAndFetchedCount(queue);

			    result.Add(new QueueWithTopEnqueuedJobsDto
			    {
				    Name = queue,
				    Length = counters.enqueuedCount,
				    Fetched = counters.fetchedCount,
				    FirstJobs = EnqueuedJobs(connection, enqueuedJobIds)
			    });
		    }

	    }

	    

	    public IList<ServerDto> Servers()
	    {
		    throw new NotImplementedException();
	    }

	    public JobDetailsDto JobDetails(string jobId)
	    {
		    throw new NotImplementedException();
	    }

	    public StatisticsDto GetStatistics()
	    {
		    throw new NotImplementedException();
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
    }
}
