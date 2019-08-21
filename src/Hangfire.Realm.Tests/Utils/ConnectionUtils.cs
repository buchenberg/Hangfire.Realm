using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.InteropServices;
using Realms;

namespace Hangfire.Realm.Tests.Utils
{
    public class ConnectionUtils
    {
        private const string DatabaseNameTemplate = @"HangfireRealmTests{0}.realm";
        private static ConcurrentDictionary<string, RealmConfiguration> Configurations 
            = new ConcurrentDictionary<string, RealmConfiguration>();
        public static RealmConfiguration GetRealmConfiguration()
        {
            var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                GetDatabaseName()); 
            return Configurations.GetOrAdd(path, p => new RealmConfiguration(p) { SchemaVersion = 1 });
        }
        private static string GetDatabaseName()
        {
            var framework = "Net46";
            if (RuntimeInformation.FrameworkDescription.Contains(".NET Core"))
            {
                framework = "NetCore";
            }
            else if (RuntimeInformation.FrameworkDescription.Contains("Mono"))
            {
                framework = "Mono";
            }
            return string.Format(DatabaseNameTemplate, framework);
        }
    }
}