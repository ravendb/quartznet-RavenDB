namespace Quartz.Impl.RavenDB
{
    public class RavenDbProviderOptions
    {
        private readonly SchedulerBuilder.PersistentStoreOptions _options;

        protected internal RavenDbProviderOptions(SchedulerBuilder.PersistentStoreOptions options)
        {
            _options = options;
        }


    }
}