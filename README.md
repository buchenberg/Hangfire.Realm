# Hangfire.Realm

[![Build status](https://ci.appveyor.com/api/projects/status/nw6k0n6yr8ycj50g?svg=true)](https://ci.appveyor.com/project/buchenberg/hangfire-realm)
![Nuget](https://img.shields.io/nuget/v/Hangfire.Realm)
![Nuget](https://img.shields.io/nuget/dt/Hangfire.Realm)
![GitHub](https://img.shields.io/github/license/buchenberg/Hangfire.Realm)

This [Hangfire](http://hangfire.io) extension adds support for using the lightweight embeddable [Realm](https://realm.io) object database.

_**Warning:** While this extension has been tested extensively in development, it has not been fully tested in production. Please use responsibly. Any developer input is appreciated._

## Installation
Package Manager:

`Install-Package Hangfire.Realm -Version 1.0.3`

.NET CLI:

`dotnet add package Hangfire.Realm --version 1.0.3`



## Usage

_**Note:** If you are using Realm for persisting other data in your application and it's running on Windows make sure you use the same Realm configuration for everything. Using seperate Realm files will result in intermittant "SetEndOfFile() failed" errors. See https://github.com/realm/realm-dotnet/issues/1906_

### .NET Core

Please see the [Hangfire.Realm.Sample.NET.Core](https://github.com/buchenberg/Hangfire.Realm/tree/master/src/Hangfire.Realm.Sample.NET.Core) project for a working example.

```csharp
public static void Main()
{
    //The path to the Realm DB file.
    string dbPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "Hangfire.Realm.Sample.NetCore.realm");

    //A standard Realm configuration.
    RealmConfiguration realmConfiguration = new RealmConfiguration(dbPath)
    {
        ShouldCompactOnLaunch = (totalBytes, usedBytes) =>
        {
            // Compact if the file is over 100MB in size and less than 50% 'used'
            var oneHundredMB = (ulong)(100 * 1024 * 1024);
            return totalBytes > oneHundredMB && (double)usedBytes / totalBytes < 0.5;
        }
    };

    //Hangfire.Realm storage options.
    RealmJobStorageOptions storageOptions = new RealmJobStorageOptions
    {
        RealmConfiguration = realmConfiguration, //Required.
        QueuePollInterval = TimeSpan.FromSeconds(1), //Optional. Defaults to TimeSpan.FromSeconds(15)
        SlidingInvisibilityTimeout = TimeSpan.FromSeconds(10), //Optional. Defaults to TimeSpan.FromMinutes(10)
        JobExpirationCheckInterval = TimeSpan.FromMinutes(1) //Optional. Defaults to TimeSpan.FromMinutes(30)
    };

    //Standard Hangfire server options.
    BackgroundJobServerOptions serverOptions = new BackgroundJobServerOptions()
    {
        WorkerCount = 10,
        Queues = new[] { "default", "critical" },
        ServerTimeout = TimeSpan.FromMinutes(10),
        HeartbeatInterval = TimeSpan.FromSeconds(30),
        ServerCheckInterval = TimeSpan.FromSeconds(10),
        SchedulePollingInterval = TimeSpan.FromSeconds(10)
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
            BackgroundJob.Enqueue(() =>
            Console.WriteLine($"Fire-and-forget job {jobNumber}"));
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
```

### ASP.NET Core

Please see the [Hangfire.Realm.Sample.ASP.NET.Core](https://github.com/buchenberg/Hangfire.Realm/tree/master/src/Hangfire.Realm.Sample.ASP.NET.Core) project for a working example.

```csharp
public class Startup
{
    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public IConfiguration Configuration { get; }

    public void ConfigureServices(IServiceCollection services)
    {
        RealmJobStorageOptions storageOptions = new RealmJobStorageOptions
        {
            RealmConfiguration = new RealmConfiguration(
              Path.Combine(
              Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
              "Some.realm"),
            QueuePollInterval = TimeSpan.FromSeconds(1),
            SlidingInvisibilityTimeout = TimeSpan.FromSeconds(10)
        };

        services.AddHangfire(config =>
        {
            config
            .UseRealmJobStorage(storageOptions)
            .UseLogProvider(new ColouredConsoleLogProvider());
        });
        services.AddHangfireServer(options =>
        {
            options.WorkerCount = 10;
            options.Queues = new[] { "default" };
            options.ServerTimeout = TimeSpan.FromMinutes(10);
            options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            options.ServerCheckInterval = TimeSpan.FromSeconds(10);
            options.SchedulePollingInterval = TimeSpan.FromSeconds(10);
        });

        services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);
    }

    public void Configure(IApplicationBuilder app, IHostingEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        else
        {
            app.UseHsts();
        }
        app.UseHangfireDashboard();
        app.UseStaticFiles();
        app.UseHttpsRedirection();
        app.UseMvc();
    }
}
```

The Hangfire web dashboard will be available at /hangfire.

## Issues

If you have any questions or issues related to Hangfire.Realm or want to discuss new features please create a new or comment on an existing [issue](https://github.com/buchenberg/Hangfire.Realm/issues).
