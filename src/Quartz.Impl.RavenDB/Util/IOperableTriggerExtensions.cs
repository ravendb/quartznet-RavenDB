using Quartz.Spi;

namespace Quartz.Impl.RavenDB.Util
{
    internal static class IOperableTriggerExtensions
    {
        public static string GetDatabaseId(this IOperableTrigger trigger)
        {
            return $"{trigger.Key.Name}/{trigger.Key.Group}";
        }

        public static string GetJobDatabaseId(this IOperableTrigger trigger)
        {
            return $"{trigger.JobKey.Name}/{trigger.JobKey.Group}";
        }
    }
}