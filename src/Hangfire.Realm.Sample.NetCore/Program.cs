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


            GlobalConfiguration.Configuration.UseLogProvider(new ColouredConsoleLogProvider());
            JobStorage.Current = new RealmJobStorage(new RealmJobStorageOptions
            {
                RealmConfiguration = new RealmConfiguration(Path.Combine(Directory.GetCurrentDirectory(), "sample.realm"))
            });

            using (new BackgroundJobServer())
            {
                for (var i = 0; i < JobCount; i++)
                {
                    var jobId = i;
                    BackgroundJob.Enqueue(() => Console.WriteLine($"Fire-and-forget ({jobId})"));
                }

                Console.WriteLine($"{JobCount} job(s) has been enqued. They will be executed shortly!");
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
