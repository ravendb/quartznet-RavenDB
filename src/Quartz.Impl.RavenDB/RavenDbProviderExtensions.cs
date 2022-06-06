using System;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Quartz.Spi;
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
        public static void UseRavenDbWithInjectedDocumentStore(this SchedulerBuilder.PersistentStoreOptions options, 
            Microsoft.Extensions.DependencyInjection.IServiceCollection services)
        {
            options.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion());

            services.AddSingleton<IJobStore, RavenJobStore>();
        }
    }
}