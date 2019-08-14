using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Hangfire.Realm.Sample.ASP.NET.Core.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class JobsController
    {
        // POST api/jobs
        [HttpPost]
        [Produces("application/json")]
        [Route("api/jobs/new")]
        public void Post([FromBody] string value)
        {
            BackgroundJobServerOptions serverOptions = new BackgroundJobServerOptions()
            {
                WorkerCount = 1,
                Queues = new[] { "critical", "default" },
                ServerTimeout = TimeSpan.FromMinutes(10),
                HeartbeatInterval = TimeSpan.FromSeconds(10),
                ServerCheckInterval = TimeSpan.FromSeconds(10),
                SchedulePollingInterval = TimeSpan.FromSeconds(10),
                //CountersAggregateInterval = TimeSpan.FromMinutes(1),
                //ExpirationCheckInterval = TimeSpan.FromMinutes(15),

            };
            using (new BackgroundJobServer(serverOptions))
            {
                for (var i = 0; i < 50; i++)
                {
                    var jobNumber = i + 1;
                    var jobId = BackgroundJob.Enqueue(() =>
                    Console.WriteLine($"Fire-and-forget job {jobNumber}"));
                    //Console.WriteLine($"Job {jobNumber} was given Id {jobId} and placed in queue");
                }

                BackgroundJob.Schedule(() =>
                Console.WriteLine("Scheduled job"),
                TimeSpan.FromSeconds(30));

            }
        }
    }
}
