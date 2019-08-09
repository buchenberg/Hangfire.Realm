using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Hangfire.Realm
{
    public interface IJobQueueSemaphore
    {
        bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue);
        void WaitNonBlock(string queue);
        void Release(string queue);
    }
    public class JobQueueSemaphore : IJobQueueSemaphore, IDisposable
    {
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void Release(string queue)
        {
            throw new NotImplementedException();
        }

        public bool WaitAny(string[] queues, CancellationToken cancellationToken, TimeSpan timeout, out string queue)
        {
            throw new NotImplementedException();
        }

        public void WaitNonBlock(string queue)
        {
            throw new NotImplementedException();
        }
    }
}
