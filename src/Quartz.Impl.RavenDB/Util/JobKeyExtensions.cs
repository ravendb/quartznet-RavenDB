namespace Quartz.Impl.RavenDB.Util
{
    internal static class JobKeyExtensions
    {
        public static string GetDatabaseId(this JobKey jobKey)
        {
            return $"{jobKey.Name}/{jobKey.Group}";
        }
    }
}