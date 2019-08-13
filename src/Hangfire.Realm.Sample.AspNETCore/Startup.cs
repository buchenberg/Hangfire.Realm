using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Logging.LogProviders;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Realms;

namespace Hangfire.Realm.Sample.AspNETCore
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
                RealmConfiguration = new RealmConfiguration(Path.Combine(Directory.GetCurrentDirectory(), "sample.realm"))
            };

            //BackgroundJobServerOptions serverOptions = new BackgroundJobServerOptions()
            //{
            //    WorkerCount = 1,
            //    Queues = new[] { "critical", "default" },
            //    ServerTimeout = TimeSpan.FromMinutes(10),
            //    HeartbeatInterval = TimeSpan.FromSeconds(10),
            //    ServerCheckInterval = TimeSpan.FromSeconds(10),
            //    SchedulePollingInterval = TimeSpan.FromSeconds(10),
                //CountersAggregateInterval = TimeSpan.FromMinutes(1),
                //ExpirationCheckInterval = TimeSpan.FromMinutes(15),

           // };
            services.AddHangfire(config =>
            {
                config
                .UseRealmJobStorage(storageOptions)
                .UseLogProvider(new ColouredConsoleLogProvider());
            });
            //services.AddHangfireServer();

            services.Configure<CookiePolicyOptions>(options =>
            {
                // This lambda determines whether user consent for non-essential cookies is needed for a given request.
                options.CheckConsentNeeded = context => true;
                options.MinimumSameSitePolicy = SameSiteMode.None;
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
                app.UseExceptionHandler("/Error");
            }
            app.UseHangfireServer();
            app.UseHangfireDashboard();
            app.UseStaticFiles();
            app.UseCookiePolicy();
            app.UseMvc();
        }
    }
}
