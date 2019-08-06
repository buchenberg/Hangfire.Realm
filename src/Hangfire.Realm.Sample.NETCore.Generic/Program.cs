using Hangfire;
using Hangfire.Logging.LogProviders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Realms;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Hangfire.Realm.Sample.NETCore.Generic
{
    class Program
    {

        private const int JobCount = 100;
        static async Task Main(string[] args)
        {
            var pathToExe = Process.GetCurrentProcess().MainModule.FileName;
            var pathToContentRoot = Path.GetDirectoryName(pathToExe);
            Directory.SetCurrentDirectory(pathToContentRoot);

            IHost host = new HostBuilder()
               .ConfigureHostConfiguration(config =>
               {

               })
               .ConfigureAppConfiguration((hostContext, config) =>
               {
                   config.SetBasePath(Directory.GetCurrentDirectory());

               })
               .ConfigureServices((hostContext, services) =>
               {

                   services.AddHangfire(config =>
                   {
                       RealmJobStorageOptions realmJobStorageOptions = new RealmJobStorageOptions
                       {
                           RealmConfiguration = new RealmConfiguration("sample.realm")
                       };
                       config.UseRealmJobStorage(realmJobStorageOptions);
                       config.UseLogProvider(new ColouredConsoleLogProvider());
                   });

                   services.AddHangfireServer();

               })
               .UseConsoleLifetime()
               .Build();

            await host.RunAsync();



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
