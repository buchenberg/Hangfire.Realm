using System;
using Realms;

namespace Hangfire.Realm.Tests.Utils
{
    public class ConnectionUtils
    {
        private static readonly string Path =$"{Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)}\\default.realm"; 
        private static readonly RealmConfiguration Configuration = new RealmConfiguration(Path);
        
        public static Realms.Realm GetRealm()
        {
            var config = Configuration;
            return Realms.Realm.GetInstance(config);
        }

        public static void DeleteRealm()
        {
            var config = Configuration;
            Realms.Realm.DeleteRealm(config);
        }
    }
}