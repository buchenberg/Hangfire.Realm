using System;

namespace Hangfire.Realm.Models
{
    public interface IExpireAt
    {
        DateTimeOffset? ExpireAt { get; set; }
    }
}