using System;
using Raven.Client.Documents;

namespace Quartz.Impl.RavenDB
{
    public class DocumentStoreHolder
    {
        private static readonly Lazy<IDocumentStore> store = new Lazy<IDocumentStore>(CreateStore);

        public static IDocumentStore Store => store.Value;

        private static IDocumentStore CreateStore()
        {
            var documentStore = new DocumentStore()
            {
                Urls = new[] { RavenJobStore.Url },
                Database = RavenJobStore.DefaultDatabase
            };

            // For multithreaded debugging need to uncomment next line (prints thread id and stack trace)
            //documentStore.RegisterListener(new ThreadPrinter());

            documentStore.Initialize();
            //documentStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;
            return documentStore;
        }
    }
}
