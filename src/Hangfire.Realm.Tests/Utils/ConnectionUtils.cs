using System;

namespace Hangfire.Realm.Tests.Utils
{
    public class ConnectionUtils
    {
        public static readonly string DatebasePath =
            $"{Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)}\\Realm";

        public static Realms.Realm GetRealm()
        {
            return Realms.Realm.GetInstance(DatebasePath);
        }
    }
}