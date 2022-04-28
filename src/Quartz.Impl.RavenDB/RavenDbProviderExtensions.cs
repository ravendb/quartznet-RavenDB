using System;
using JetBrains.Annotations;
using Quartz.Util;

namespace Quartz.Impl.RavenDB
{
    public static class RavenDbProviderExtensions
    {
        [UsedImplicitly]
        public static void UseRavenDb(this SchedulerBuilder.PersistentStoreOptions options,
            Action<RavenDbProviderOptions> config = null)
        {
            options.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion());
            config?.Invoke(new RavenDbProviderOptions(options));
        }

        [UsedImplicitly]
        public static void UseRavenDbWithInjectedDocumentStore(this SchedulerBuilder.PersistentStoreOptions options)
        {
            options.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(RavenJobStoreWithInjectDocumentStore).AssemblyQualifiedNameWithoutVersion());
        }
    }
}