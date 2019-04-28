using System;

namespace Hangfire.Realm
{
    public class RealmDistributedLockException : Exception
    {
        public RealmDistributedLockException(string message, Exception innerException)
            : base(message, innerException)
        {
            
        }
    }
}