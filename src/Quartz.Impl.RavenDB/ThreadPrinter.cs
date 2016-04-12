using System;
using System.Threading;

using Raven.Client.Listeners;
using Raven.Json.Linq;

namespace Quartz.Impl.RavenDB
{
    public class ThreadPrinter : IDocumentStoreListener
    {
        public bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original)
        {
            Console.WriteLine("PUT of " + key + " on " + Thread.CurrentThread.ManagedThreadId);
            Console.WriteLine("Stack trace:" + Environment.StackTrace);
            return false;
        }

        public void AfterStore(string key, object entityInstance, RavenJObject metadata)
        {
        }
    }
}