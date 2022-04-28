using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB
{
    public class RavenJobStoreWithInjectDocumentStore : RavenJobStore
    {
        private readonly IServiceProvider _provider;

        public RavenJobStoreWithInjectDocumentStore(IServiceProvider provider)
        {
            _provider = provider;
        }

        protected override IDocumentStore InitializeDocumentStore()
        {
            var store = _provider.GetService(typeof(IDocumentStore));
            if (store == null)
                throw new InvalidOperationException("Unable to get IDocumentStore from the ServiceProvider");
            return (IDocumentStore)store;
        }
    }
}
