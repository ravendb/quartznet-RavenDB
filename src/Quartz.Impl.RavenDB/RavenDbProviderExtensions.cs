using System;
using Quartz.Spi;
using Quartz.Util;

namespace Quartz.Impl.RavenDB
{
    public static class RavenDbProviderExtensions
    {
        /*
        /// <summary>
        ///     Use <see cref="RavenJobStore"/> as persistent <see cref="IJobStore"/>.
        /// </summary>
        public static IPropertyConfigurer UseRavenDbStore(this IPropertyConfigurer builder, Action<RavenDbStoreOptions>? options = null)
        {
            builder.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion());
            options?.Invoke(new RavenDbStoreOptions(builder));
            return builder;
        }
        */

        public static void UseRavenDb(this SchedulerBuilder.PersistentStoreOptions options,
            Action<RavenDbProviderOptions> config = null)
        {
            options.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion());
            config?.Invoke(new RavenDbProviderOptions(options));
        }
    }
}