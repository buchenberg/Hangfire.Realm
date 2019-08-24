using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.IO;
using Hangfire.Logging.LogProviders;
using Microsoft.AspNetCore.Http;
using Realms;

namespace Hangfire.Realm.Sample.ASP.NET.Core
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            RealmJobStorageOptions storageOptions = new RealmJobStorageOptions
            {
                RealmConfiguration = new RealmConfiguration(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Hangfire.Realm.Sample.NetCore.realm")),
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

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }
            app.UseHangfireDashboard();
            app.UseStaticFiles();
            app.UseHttpsRedirection();
            app.UseMvc();
        }
    }
}
