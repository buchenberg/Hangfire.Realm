using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.Realm
{
    public class RealmJobQueueProviderCollection : IEnumerable<RealmJobQueueProvider>
    {
        private readonly List<RealmJobQueueProvider> _providers
           = new List<RealmJobQueueProvider>();
        private readonly Dictionary<string, RealmJobQueueProvider> _providersByQueue
            = new Dictionary<string, RealmJobQueueProvider>(StringComparer.OrdinalIgnoreCase);

        private readonly RealmJobQueueProvider _defaultProvider;

        public RealmJobQueueProviderCollection(RealmJobQueueProvider defaultProvider)
        {
            if (defaultProvider == null) throw new ArgumentNullException(nameof(defaultProvider));

            _defaultProvider = defaultProvider;

            _providers.Add(_defaultProvider);
        }

        public void Add(RealmJobQueueProvider provider, IEnumerable<string> queues)
        {
            if (provider == null) throw new ArgumentNullException(nameof(provider));
            if (queues == null) throw new ArgumentNullException(nameof(queues));

            _providers.Add(provider);

            foreach (var queue in queues)
            {
                _providersByQueue.Add(queue, provider);
            }
        }

        public RealmJobQueueProvider GetProvider(string queue)
        {
            return _providersByQueue.ContainsKey(queue)
                ? _providersByQueue[queue]
                : _defaultProvider;
        }

        public IEnumerator<RealmJobQueueProvider> GetEnumerator()
        {
            return _providers.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
