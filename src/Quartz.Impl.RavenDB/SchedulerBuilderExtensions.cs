using System;
using Quartz.Util;

namespace Quartz.Impl.RavenDB
{
    public static class SchedulerBuilderExtensions
    {
        public static SchedulerBuilder UseRavenDbStore(this SchedulerBuilder builder, Action<RavenDbStoreOptions>? options = null)
        {
            builder.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(RavenJobStore).AssemblyQualifiedNameWithoutVersion());
            options?.Invoke(new RavenDbStoreOptions(builder));
            return builder;
        }
    }
}