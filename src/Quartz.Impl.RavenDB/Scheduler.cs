using System;
using System.Collections.Generic;


namespace Quartz.Impl.RavenDB
{
    public class Scheduler
    {
        public string InstanceName { get; set; }
        public DateTimeOffset LastCheckinTime { get; set; }
        public DateTimeOffset CheckinInterval { get; set; }
        public string State { get; set; }
        public Dictionary<string, ICalendar> Calendars { get; set; }
        public Collection.HashSet<string> PausedJobGroups { get; set; }
        public Collection.HashSet<string> BlockedJobs { get; set; }
    }
}  