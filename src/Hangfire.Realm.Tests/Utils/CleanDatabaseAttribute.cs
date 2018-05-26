using System.Reflection;
using System.Threading;
using Xunit.Sdk;

namespace Hangfire.Realm.Tests.Utils
{
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();

        public bool Initialized { get; }
        
        public CleanDatabaseAttribute() : this(true)
        {
        }

        public CleanDatabaseAttribute(bool initialized)
        {
            Initialized = initialized;
        }
        
        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);

            var realm = ConnectionUtils.GetRealm();
            if (Initialized)
            {
                realm.RemoveAll();
                return;
            }

            // Drop the database and do not run any
            // migrations to initialize the database.
            
            realm.RemoveAll();
        }
        
        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(GlobalLock);
        }
    }
}