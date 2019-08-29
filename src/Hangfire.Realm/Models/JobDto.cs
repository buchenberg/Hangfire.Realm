using System;
using System.Collections.Generic;
using Realms;

namespace Hangfire.Realm.Models
{
	public class JobDto : RealmObject, IExpireAt
    {
        [PrimaryKey]
        public string Id { get; set; } = Guid.NewGuid().ToString();


        public DateTimeOffset Created { get; set; } = DateTimeOffset.Now;

        public string StateName { get; set; }

	    public string InvocationData { get; set; }

	    public string Arguments { get; set; }

	    public IList<ParameterDto> Parameters { get; }

	    public IList<StateDto> StateHistory { get; }

	    public DateTimeOffset? ExpireAt { get; set; }

	    public override string ToString()
	    {
		    return Id + " " + StateName;
	    }
    }
}
