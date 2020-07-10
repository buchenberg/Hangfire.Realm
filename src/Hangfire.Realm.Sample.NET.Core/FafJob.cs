using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Hangfire.Realm.Sample.NET.Core
{
    internal class FafJob
    {

        public void Execute(int jobNumber, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Thread.Sleep(1000);
            Console.WriteLine($"Fire-and-forget job {jobNumber}");
        }
    }
}
