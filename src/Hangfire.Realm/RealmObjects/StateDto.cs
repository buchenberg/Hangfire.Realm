using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.RealmObjects
{
	internal class StateDto : RealmObject
    {
	    public string Name { get; set; }

	    public string Reason { get; set; }

	    public DateTimeOffset Created { get; set; }

	    public IList<KeyValueDto> Data { get; }
    }
}
