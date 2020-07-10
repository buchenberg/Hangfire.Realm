using System;
using System.IO;
using System.Threading;
using Hangfire.Logging.LogProviders;
using Realms;

namespace Hangfire.Realm.Sample.NET.Core
{
    public class Program
    {
        private const int JobCount = 10;


        public static void Main()
        {
            //The path to the Realm DB file.
            string dbPath = Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                        "Hangfire.Realm.Sample.NetCore.realm");
            Console.WriteLine($"Using database {dbPath}");

            //A standard Realm configuration.
            RealmConfiguration realmConfiguration = new RealmConfiguration(dbPath)
            {
                ShouldCompactOnLaunch = (totalBytes, usedBytes) =>
                {
                    // Compact if the file is over 100MB in size and less than 50% 'used'
                    var oneHundredMB = (ulong)(100 * 1024 * 1024);
                    return totalBytes > oneHundredMB && (double)usedBytes / totalBytes < 0.5;
                },
            };

            //Hangfire.Realm storage options. 
            RealmJobStorageOptions storageOptions = new RealmJobStorageOptions
            {
                RealmConfiguration = realmConfiguration, //Required.
                QueuePollInterval = TimeSpan.FromSeconds(1), //Optional. Defaults to TimeSpan.FromSeconds(15)
                //SlidingInvisibilityTimeout = TimeSpan.FromSeconds(10), //Optional. Defaults to TimeSpan.FromMinutes(10)
                JobExpirationCheckInterval = TimeSpan.FromMinutes(1) //Optional. Defaults to TimeSpan.FromMinutes(30)
            };

            //Standard Hangfire server options. 
            BackgroundJobServerOptions serverOptions = new BackgroundJobServerOptions()
            {
                WorkerCount = 40,
                Queues = new[] { "default"},
                ServerTimeout = TimeSpan.FromMinutes(10),
                HeartbeatInterval = TimeSpan.FromSeconds(60),
                ServerCheckInterval = TimeSpan.FromSeconds(10),
                SchedulePollingInterval = TimeSpan.FromSeconds(10),

            };

            //Hangfire global configuration
            GlobalConfiguration.Configuration
            .UseLogProvider(new ColouredConsoleLogProvider(Logging.LogLevel.Debug))
            .UseRealmJobStorage(storageOptions);


            using (new BackgroundJobServer(serverOptions))
            {

                //Queue a bunch of fire-and-forget jobs
                for (var i = 0; i < JobCount; i++)
                {
                    var jobNumber = i + 1;
                    BackgroundJob.Enqueue<FafJob>((_) => _.Execute(jobNumber, CancellationToken.None));

                }

                //A scheduled job that will run 1.5 minutes after being placed in queue
                BackgroundJob.Schedule(() =>
                Console.WriteLine("A Scheduled job."),
                TimeSpan.FromMinutes(1.5));

                //A fire-and-forget continuation job that has three steps
                BackgroundJob.ContinueJobWith(
                  BackgroundJob.ContinueJobWith(
                    BackgroundJob.Enqueue(
                         () => Console.WriteLine($"Knock knock..")),
                           () => Console.WriteLine("Who's there?")),
                             () => Console.WriteLine("A continuation job!"));

                //A scheduled continuation job that has three steps
                BackgroundJob.ContinueJobWith(
                  BackgroundJob.ContinueJobWith(
                    BackgroundJob.Schedule(
                         () => Console.WriteLine($"Knock knock.."), TimeSpan.FromMinutes(2)),
                           () => Console.WriteLine("Who's there?")),
                             () => Console.WriteLine("A scheduled continuation job!"));

                //A Cron based recurring job
                RecurringJob.AddOrUpdate("recurring-job-1", () =>
                Console.WriteLine("Recurring job 1."),
                Cron.Minutely);

                //Another recurring job
                RecurringJob.AddOrUpdate("recurring-job-2", () =>
                Console.WriteLine("Recurring job 2."),
                Cron.Minutely);

                //An update to the first recurring job
                RecurringJob.AddOrUpdate("recurring-job-1", () =>
                Console.WriteLine("Recurring job 1 (edited)."),
                Cron.Minutely);

                Console.Read();
            }
        }
    }
}
