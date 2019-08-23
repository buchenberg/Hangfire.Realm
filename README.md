# Hangfire.Realm

This [Hangfire](http://hangfire.io) extension adds support for using the lightweight embeddable [Realm](https://realm.io) object database.

_**Warning:** This project is under active development and has not been fully tested in production. Please use responsibly. Hangfire Continuations are not currently supported for recurring jobs. Any developer input is appreciated._

## Installation

This project is not yet available as a NuGet package. Install it by checking it out with Git or downloading it directly as a zip and adding a reference to the Hangfire.Realm project to your code.

## Usage

### .NET Core

Please see the [Hangfire.Realm.Sample.NET.Core](https://github.com/gottscj/Hangfire.Realm/tree/master/src/Hangfire.Realm.Sample.NET.Core) project for a working example.

```csharp
public static void Main()
{
    RealmJobStorageOptions storageOptions = new RealmJobStorageOptions
    {
        RealmConfiguration = new RealmConfiguration(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Some.realm")),
        QueuePollInterval = TimeSpan.FromSeconds(1),
        SlidingInvisibilityTimeout = TimeSpan.FromSeconds(10)
    };

    BackgroundJobServerOptions serverOptions = new BackgroundJobServerOptions()
    {
        WorkerCount = 10,
        Queues = new[] { "default" },
        ServerTimeout = TimeSpan.FromMinutes(10),
        HeartbeatInterval = TimeSpan.FromSeconds(30),
        ServerCheckInterval = TimeSpan.FromSeconds(10),
        SchedulePollingInterval = TimeSpan.FromSeconds(10)
    };

    GlobalConfiguration.Configuration
    .UseLogProvider(new ColouredConsoleLogProvider(Logging.LogLevel.Debug))
    .UseRealmJobStorage(storageOptions);

    using (new BackgroundJobServer(serverOptions))
    {
        BackgroundJob.Enqueue(() =>
            Console.WriteLine($"Fire-and-forget job"));

        BackgroundJob.Schedule(() =>
        Console.WriteLine("Scheduled job"),
        TimeSpan.FromSeconds(60));

        RecurringJob.AddOrUpdate("some-recurring-job", () =>
        Console.WriteLine("Recurring job"),
        Cron.Minutely);
        BackgroundJob.ContinueJobWith(
                  BackgroundJob.ContinueJobWith(
                    BackgroundJob.Enqueue(
                         () => Console.WriteLine($"Knock knock..")),
                           () => Console.WriteLine("Who's there?")),
                             () => Console.WriteLine("A continuation job!"));

        Console.Read();
    }
}
```

### ASP.NET Core

Please see the [Hangfire.Realm.Sample.ASP.NET.Core](https://github.com/gottscj/Hangfire.Realm/tree/master/src/Hangfire.Realm.Sample.ASP.NET.Core) project for a working example.

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

If you have any questions or issues related to Hangfire.Realm or want to discuss new features please create a new or comment on an existing [issue](https://github.com/gottscj/Hangfire.Realm/issues).
