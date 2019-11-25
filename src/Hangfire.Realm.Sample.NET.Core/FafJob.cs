using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Hangfire.Realm.Sample.NET.Core
{
    internal class FafJob
    {

        public void Execute(int jobNumber, IJobCancellationToken cancellationToken)
        {
            for (var i = 0; i < 10; i++)
            {
                if (null != cancellationToken) cancellationToken.ThrowIfCancellationRequested();
                Thread.Sleep(5000);
                Console.WriteLine($"Fire-and-forget job {jobNumber} - {i + 1}");
            }
        }
     }
}
