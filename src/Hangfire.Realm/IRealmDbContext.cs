using System;
using Realms;

namespace Hangfire.Realm
{
    public interface IRealmDbContext
    {
        Realms.Realm GetRealm();
    }
    
    public class RealmDbContext : IRealmDbContext
    {
        private readonly RealmConfigurationBase _realmConfiguration;

        public RealmDbContext(RealmConfigurationBase realmConfiguration)
        {
            _realmConfiguration = realmConfiguration ?? throw new ArgumentNullException(nameof(realmConfiguration));
        }
        public Realms.Realm GetRealm()
        {
            return Realms.Realm.GetInstance(_realmConfiguration);
        }

    }
}