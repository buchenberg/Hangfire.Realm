using Hangfire.Annotations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.Realm
{
    public static class RealmJobStorageExtensions
    {

        public static IGlobalConfiguration<RealmJobStorage> UseRealmJobStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] RealmJobStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (options == null) throw new ArgumentNullException(nameof(options));

            var storage = new RealmJobStorage(options);
            return configuration.UseStorage(storage);
        }
    }
}
