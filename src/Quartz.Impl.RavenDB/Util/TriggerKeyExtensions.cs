namespace Quartz.Impl.RavenDB.Util
{
    internal static class TriggerKeyExtensions
    {
        public static string GetDatabaseId(this TriggerKey triggerKey)
        {
            return $"{triggerKey.Name}/{triggerKey.Group}";
        }
    }
}