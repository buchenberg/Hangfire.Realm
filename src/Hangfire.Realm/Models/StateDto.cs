using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Models
{
	internal class StateDto : RealmObject
    {
	    public string Name { get; set; }

	    public string Reason { get; set; }

	    public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;

	    public IList<StateDataDto> Data { get; }
    }
}
