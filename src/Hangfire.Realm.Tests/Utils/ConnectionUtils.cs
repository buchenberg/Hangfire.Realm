using System;
using Realms;

namespace Hangfire.Realm.Tests.Utils
{
    public class ConnectionUtils
    {
        public static readonly string DatebasePath =
            $"{Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData)}\\default.realm";

        public static Realms.Realm GetRealm()
        {
            return Realms.Realm.GetInstance(DatebasePath);
        }
    }
}