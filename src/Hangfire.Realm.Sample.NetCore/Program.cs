using Hangfire;
using Hangfire.Logging.LogProviders;
using Realms;
using System;
using System.IO;

namespace Hangfire.Realm.Sample.NetCore
{
    public class Program
    {
        private const int JobCount = 100;
        

        public static void Main()
        {
            var dbPath = Path.Combine(Directory.GetCurrentDirectory(), "sample.realm");

            var hfConfig = GlobalConfiguration.Configuration;
            hfConfig.UseLogProvider(new ColouredConsoleLogProvider());
            hfConfig.UseRealmJobStorage(new RealmJobStorageOptions
            {
                RealmConfiguration = new RealmConfiguration(dbPath)
            });

            var options = new BackgroundJobServerOptions()
            {
                ServerTimeout = TimeSpan.FromMinutes(10),
                HeartbeatInterval = TimeSpan.FromSeconds(10),
                ServerCheckInterval = TimeSpan.FromSeconds(10),
                SchedulePollingInterval = TimeSpan.FromSeconds(10),
                Queues = new[] { "critical", "default" }
            };
            using (new BackgroundJobServer(options))
            {
                for (var i = 0; i < JobCount; i++)
                {
                    var jobNumber = i + 1;
                    var jobId = BackgroundJob.Enqueue(() => Console.WriteLine($"Fire-and-forget job {jobNumber}"));
                    Console.WriteLine($"Job {jobNumber} was given Id {jobId} and placed in queue");
                }
                //BackgroundJob.Schedule(() => 
                //Console.WriteLine("Scheduled job"),
                //TimeSpan.FromSeconds(30));

                Console.WriteLine($"{JobCount} job(s) has been enqueued. They will be executed shortly!");
                Console.WriteLine();
                Console.WriteLine("If you close this application before they are executed, ");
                Console.WriteLine("they will be executed the next time you run this sample.");
                Console.WriteLine();
                Console.WriteLine("Press [enter] to exit...");

                Console.Read();
            }
        }
    }
}
