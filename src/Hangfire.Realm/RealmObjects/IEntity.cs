using System;

namespace Hangfire.Realm.RealmObjects
{
    internal interface IEntity
    {
        string Id { get; set; }
        
        DateTimeOffset Created { get; set; }
    }
}