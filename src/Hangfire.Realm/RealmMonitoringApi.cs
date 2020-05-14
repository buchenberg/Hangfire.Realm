using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Hangfire.Common;
using Hangfire.Realm.DAL;
using Hangfire.Realm.Models;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;

namespace Hangfire.Realm
{
    internal class RealmMonitoringApi : IMonitoringApi
    {

        private readonly RealmJobStorage _storage;

        public RealmMonitoringApi(RealmJobStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {

            var queues = GetQueues();

            var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);
            foreach (var queue in queues)
            {
                var enqueuedJobIds = GetEnqueuedJobIds(queue, 0, 5);
                var (enqueuedCount, fetchedCount) = GetEnqueuedAndFetchedCount(queue);
                var enqueuedJobs = GetEnqueuedJobs(enqueuedJobIds);

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue,
                    Length = enqueuedCount,
                    Fetched = fetchedCount,
                    FirstJobs = enqueuedJobs
                });
            }

            return result;



        }

        public IList<Storage.Monitoring.ServerDto> Servers()
        {
            using (var realm = _storage.GetRealm())
            {
                var servers = realm.All<Models.ServerDto>()
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

        }

        public JobDetailsDto JobDetails(string jobId)
        {
            using (var realm = _storage.GetRealm())
            {
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
        }

        public StatisticsDto GetStatistics()
        {
            using (var realm = _storage.GetRealm())
            {
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


                stats.Queues = GetQueues().Count;

                return stats;
            }
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var jobIds = GetEnqueuedJobIds(queue, from, perPage);
            var jobs = GetEnqueuedJobs(jobIds);
            return jobs;
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var jobIds = GetFetchedJobIds(queue, from, perPage);
            var jobs = GetFetchedJobs(jobIds);
            return jobs;
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {

            var jobs = GetJobsByStateName(ProcessingState.StateName, from, count);

            return GetJobs(jobs, ProcessingState.StateName, (job, stateData, stateReason) => new ProcessingJobDto
            {
                Job = job,
                ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                StartedAt = JobHelper.DeserializeDateTime(stateData["StartedAt"])
            });


        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {

            var jobs = GetJobsByStateName(ScheduledState.StateName, from, count);

            return GetJobs(jobs, ScheduledState.StateName, (job, stateData, stateReason) => new ScheduledJobDto
            {
                Job = job,
                EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                ScheduledAt = JobHelper.DeserializeDateTime(stateData["ScheduledAt"])
            });

        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            var jobs = GetJobsByStateName(SucceededState.StateName, from, count);

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
            var jobs = GetJobsByStateName(FailedState.StateName, from, count);

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
            var jobs = GetJobsByStateName(DeletedState.StateName, from, count);

            return GetJobs(jobs, DeletedState.StateName, (job, stateData, stateReason) => new DeletedJobDto
            {
                Job = job,
                DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
            });
        }

        public long ScheduledCount()
        {
            return GetJobCountByStateName(ScheduledState.StateName);
        }

        public long EnqueuedCount(string queue)
        {
            using (var realm = _storage.GetRealm())
            {
                return realm.All<JobQueueDto>().Count(j => j.Queue == queue && j.FetchedAt == null);
            }

        }

        public long FetchedCount(string queue)
        {
            using (var realm = _storage.GetRealm())
            {
                return realm.All<JobQueueDto>().Count(j => j.Queue == queue && j.FetchedAt != null);
            }
        }

        public long FailedCount()
        {

            return GetJobCountByStateName(FailedState.StateName);

        }

        public long ProcessingCount()
        {
            return GetJobCountByStateName(ProcessingState.StateName);
        }

        public long SucceededListCount()
        {
            return GetJobCountByStateName(SucceededState.StateName);
        }

        public long DeletedListCount()
        {
            return GetJobCountByStateName(DeletedState.StateName);
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return GetTimelineStats(SucceededState.StateName.ToLower());
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return GetTimelineStats(FailedState.StateName.ToLower());
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return GetHourlyTimelineStats(SucceededState.StateName.ToLower());
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return GetHourlyTimelineStats(FailedState.StateName.ToLower());
        }
        #region private
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

        private IList<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            using (var realm = _storage.GetRealm())
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
        }
        private JobList<EnqueuedJobDto> GetEnqueuedJobs(IList<string> jobIds)
        {
            using (var realm = _storage.GetRealm())
            {
                var jobs = FindJobs(jobIds);

                var enqueuedJobs = FindQueuedJobs(jobIds, enqueued: true);

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
        }

        private (int enqueuedCount, int fetchedCount) GetEnqueuedAndFetchedCount(string queue)
        {
            using (var realm = _storage.GetRealm())
            {
                var enqueuedCount = realm.All<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt == null);

                var fetchedCount = realm.All<JobQueueDto>().Count(q => q.Queue == queue && q.FetchedAt != null);

                return (enqueuedCount, fetchedCount);
            }
        }
        private IList<string> GetQueues()
        {
            using (var realm = _storage.GetRealm())
            {
                return realm
                    .All<JobQueueDto>()
                    .ToList()
                    .Select(j => j.Queue)
                    .ToList();
            }
        }
        private IList<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            using (var realm = _storage.GetRealm())
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
        }
        private JobList<FetchedJobDto> GetFetchedJobs(ICollection<string> jobIds)
        {
            using (var realm = _storage.GetRealm())
            {
                var jobs = FindJobs(jobIds);
                var foundJobIds = jobs.Select(j => j.Id).ToList();

                var jobIdToJobQueueMap = FindQueuedJobs(foundJobIds, enqueued: false)
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
        }
        private IList<JobDto> GetJobsByStateName(string stateName, int from, int perPage)
        {
            using (var realm = _storage.GetRealm())
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
        }

        private long GetJobCountByStateName(string stateName)
        {
            using (var realm = _storage.GetRealm())
            {
                var count = realm.All<JobDto>().Count(j => j.StateName == stateName);
                return count;
            }
        }
        private Dictionary<DateTime, long> GetTimelineStats(string type)
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

                return CreateTimeLineStats(names, dates);

        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {

            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var names = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}").ToList();

            return CreateTimeLineStats(names, dates);
        }
        private Dictionary<DateTime, long> CreateTimeLineStats(
            IEnumerable<string> keys, IList<DateTime> dates)
        {
            using (var realm = _storage.GetRealm())
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
        }
        private IList<JobDto> FindJobs(ICollection<string> jobIds)
        {
            if (jobIds.Count == 0)
            {
                return Enumerable.Empty<JobDto>().ToList();
            }

            using (var realm = _storage.GetRealm())
            {
                var allJobs = realm.All<JobDto>();

                var param = Expression.Parameter(typeof(JobDto), "p");

                // ReSharper disable once AssignNullToNotNullAttribute
                var jobIdExp = Expression.Property(param, typeof(JobDto).GetProperty(nameof(JobDto.Id)));

                var filterExp = CreateOrElsExpression(jobIdExp, jobIds);

                return Query(allJobs, filterExp, param);
            }
        }

        private IList<JobQueueDto> FindQueuedJobs(ICollection<string> jobIds, bool enqueued)
        {
            if (jobIds.Count == 0)
            {
                return Enumerable.Empty<JobQueueDto>().ToList();
            }

            using (var realm = _storage.GetRealm())
            {
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

                var equalExp = enqueued
                    ? Expression.Equal(fetchedAtExp, Expression.Constant(null))
                    : Expression.NotEqual(fetchedAtExp, Expression.Constant(null));

                filterExp = Expression.AndAlso(equalExp, filterExp);

                return Query(allJobs, filterExp, param);
            }
        }

        private static Expression CreateOrElsExpression(MemberExpression memberExpression, IEnumerable<string> jobIds)
        {
            return jobIds
                .Select(jobId => Expression.Equal(memberExpression, Expression.Constant(jobId)))
                .Aggregate<BinaryExpression, Expression>(null, (current, equal) => current == null ? equal : 
                    Expression.OrElse(current, equal));
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
                new[] { typeof(T) },
                queryable.Expression,
                Expression.Lambda<Func<T, bool>>(filter, param));


            // Create an executable query from the expression tree.
            return queryable.Provider.CreateQuery<T>(whereCallExpression).ToList();
        }

        private static DateTime? GetEnqueudAt(JobDto jobDto)
        {
            var state = jobDto.StateHistory.LastOrDefault();
            if (state != null)
            {
                return jobDto.StateName == EnqueuedState.StateName
                    ? JobHelper.DeserializeNullableDateTime(state.Data.First(d => d.Key == "EnqueuedAt").Value)
                    : null;
            }

            return null;

        }

        #endregion

    }
}
