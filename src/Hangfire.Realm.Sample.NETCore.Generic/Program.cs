using Hangfire.Logging.LogProviders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Realms;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Hangfire.Realm.Sample.NETCore.Generic
{
    class Program
    {

        private const int JobCount = 100;
        static async Task Main(string[] args)
        {

            IHost host = new HostBuilder()
               .ConfigureAppConfiguration((hostContext, config) =>
               {
                   config.SetBasePath(Directory.GetCurrentDirectory());

               })
               .ConfigureServices((hostContext, services) =>
               {
                   services.AddHangfire(config =>
                   {
                       config.UseRealmJobStorage(new RealmJobStorageOptions
                       {
                           RealmConfiguration = new RealmConfiguration(Path.Combine(Directory.GetCurrentDirectory(), "sample.realm"))
                       });
                       config.UseLogProvider(new ColouredConsoleLogProvider());
                   });
                   services.AddHangfireServer();

               })
               .UseConsoleLifetime()
               .Build();

            await host.RunAsync();

            using (new BackgroundJobServer())
            {
                using (new BackgroundJobServer())
                {
                    Console.WriteLine("Hangfire Server started. Press ENTER to exit...");
                    Console.ReadLine();
                }
                //for (var i = 0; i < JobCount; i++)
                //{
                //    var jobId = i;
                //    BackgroundJob.Enqueue(() => Console.WriteLine($"Fire-and-forget ({jobId})"));
                //}

                //Console.WriteLine($"{JobCount} job(s) has been enqued. They will be executed shortly!");
                //Console.WriteLine();
                //Console.WriteLine("If you close this application before they are executed, ");
                //Console.WriteLine("they will be executed the next time you run this sample.");
                //Console.WriteLine();
                //Console.WriteLine("Press [enter] to exit...");

                //Console.Read();
            }
        }
    }
}
