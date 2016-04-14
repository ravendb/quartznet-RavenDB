using System;
using System.Collections.Generic;

using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl.RavenDB
{
    public class Trigger
    {
        public string Name { get; set; }
        public string Group { get; set; }
        public string Key { get; set; }

        public string JobName { get; set; }
        public string JobKey { get; set; }
        public string Scheduler { get; set; }

        public InternalTriggerState State { get; set; }
        public string Description { get; set; }
        public string CalendarName { get; set; }
        public IDictionary<string, object> JobDataMap { get; set; }
        public string FireInstanceId { get; set; }
        public int MisfireInstruction { get; set; }
        public DateTimeOffset? FinalFireTimeUtc { get; set; }
        public DateTimeOffset? EndTimeUtc { get; set; }
        public DateTimeOffset StartTimeUtc { get; set; }
        public DateTimeOffset? NextFireTimeUtc { get; set; }
        // Used for sorting triggers by time - more efficient than sorting strings
        public long NextFireTimeTicks { get; set; }

        public DateTimeOffset? PreviousFireTimeUtc { get; set; }
        public int Priority { get; set; }
        public bool HasMillisecondPrecision { get; set; }

        public CronOptions Cron { get; set; }
        public SimpleOptions Simp { get; set; }
        public CalendarOptions Cal { get; set; }
        public DailyTimeOptions Day { get; set; }

        public class CronOptions
        {
            public string CronExpression { get; set; }
            public string TimeZoneId { get; set; }
        }

        public class SimpleOptions
        {
            public int RepeatCount { get; set; }
            public TimeSpan RepeatInterval { get; set; }
        }

        public class CalendarOptions
        {
            public IntervalUnit RepeatIntervalUnit { get; set; }
            public int RepeatInterval { get; set; }
            public int TimesTriggered { get; set; }
            public string TimeZoneId { get; set; }
            public bool PreserveHourOfDayAcrossDaylightSavings { get; set; }
            public bool SkipDayIfHourDoesNotExist { get; set; }
        }

        public class DailyTimeOptions
        {
            public int RepeatCount { get; set; }
            public IntervalUnit RepeatIntervalUnit { get; set; }
            public int RepeatInterval { get; set; }
            public TimeOfDay StartTimeOfDay { get; set; }
            public TimeOfDay EndTimeOfDay { get; set; }
            public Collection.ISet<DayOfWeek> DaysOfWeek { get; set; }
            public int TimesTriggered { get; set; }
            public string TimeZoneId { get; set; }

        }

        public Trigger(IOperableTrigger newTrigger, string schedulerInstanceName)
        {
            if (newTrigger == null) return;

            Name = newTrigger.Key.Name;
            Group = newTrigger.Key.Group;
            Key = Name + "/" + Group;

            JobName = newTrigger.JobKey.Name;
            JobKey = JobName + "/" + newTrigger.JobKey.Group;
            Scheduler = schedulerInstanceName;

            State = InternalTriggerState.Waiting;
            Description = newTrigger.Description;
            CalendarName = newTrigger.CalendarName;
            JobDataMap = newTrigger.JobDataMap.WrappedMap;
            FinalFireTimeUtc = newTrigger.FinalFireTimeUtc;
            MisfireInstruction = newTrigger.MisfireInstruction;
            Priority = newTrigger.Priority;
            HasMillisecondPrecision = newTrigger.HasMillisecondPrecision;
            FireInstanceId = newTrigger.FireInstanceId;
            EndTimeUtc = newTrigger.EndTimeUtc;
            StartTimeUtc = newTrigger.StartTimeUtc;
            NextFireTimeUtc = newTrigger.GetNextFireTimeUtc();
            PreviousFireTimeUtc = newTrigger.GetPreviousFireTimeUtc();

            if (NextFireTimeUtc != null)
            {
                NextFireTimeTicks = NextFireTimeUtc.Value.UtcTicks;
            }

            // Init trigger specific properties according to type of newTrigger. 
            // If an option doesn't apply to the type of trigger it will stay null by default.

            var cronTriggerImpl = newTrigger as CronTriggerImpl;
            if (cronTriggerImpl != null)
            {
                Cron = new CronOptions
                {
                    CronExpression = cronTriggerImpl.CronExpressionString,
                    TimeZoneId = cronTriggerImpl.TimeZone.Id
                };
                return;
            }

            var simpTriggerImpl = newTrigger as SimpleTriggerImpl;
            if (simpTriggerImpl != null)
            {
                Simp = new SimpleOptions
                {
                    RepeatCount = simpTriggerImpl.RepeatCount,
                    RepeatInterval = simpTriggerImpl.RepeatInterval
                };
                return;
            }

            var calTriggerImpl = newTrigger as CalendarIntervalTriggerImpl;
            if (calTriggerImpl != null)
            {
                Cal = new CalendarOptions
                {
                    RepeatIntervalUnit = calTriggerImpl.RepeatIntervalUnit,
                    RepeatInterval = calTriggerImpl.RepeatInterval,
                    TimesTriggered = calTriggerImpl.TimesTriggered,
                    TimeZoneId = calTriggerImpl.TimeZone.Id,
                    PreserveHourOfDayAcrossDaylightSavings = calTriggerImpl.PreserveHourOfDayAcrossDaylightSavings,
                    SkipDayIfHourDoesNotExist = calTriggerImpl.SkipDayIfHourDoesNotExist
                };
                return;
            }

            var dayTriggerImpl = newTrigger as DailyTimeIntervalTriggerImpl;
            if (dayTriggerImpl != null)
            {
                Day = new DailyTimeOptions
                {
                    RepeatCount = dayTriggerImpl.RepeatCount,
                    RepeatIntervalUnit = dayTriggerImpl.RepeatIntervalUnit,
                    RepeatInterval = dayTriggerImpl.RepeatInterval,
                    StartTimeOfDay = dayTriggerImpl.StartTimeOfDay,
                    EndTimeOfDay = dayTriggerImpl.EndTimeOfDay,
                    DaysOfWeek = dayTriggerImpl.DaysOfWeek,
                    TimesTriggered = dayTriggerImpl.TimesTriggered,
                    TimeZoneId = dayTriggerImpl.TimeZone.Id
                };
            }
        }

        public IOperableTrigger Deserialize()
        {
            var triggerBuilder = TriggerBuilder.Create()
               .WithIdentity(Name, Group)
               .WithDescription(Description)
               .ModifiedByCalendar(CalendarName)
               .WithPriority(Priority)
               .StartAt(StartTimeUtc)
               .EndAt(EndTimeUtc)
               .ForJob(new JobKey(JobName, Group))
               .UsingJobData(new JobDataMap(JobDataMap));


            if (Cron != null)
            {
                triggerBuilder = triggerBuilder.WithCronSchedule(Cron.CronExpression, builder =>
                {
                    builder
                        .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(Cron.TimeZoneId));
                });
            }
            else if (Simp != null)
            {
                triggerBuilder = triggerBuilder.WithSimpleSchedule(builder =>
                {
                    builder
                        .WithInterval(Simp.RepeatInterval)
                        .WithRepeatCount(Simp.RepeatCount);
                });
            }
            else if (Cal != null)
            {
                triggerBuilder = triggerBuilder.WithCalendarIntervalSchedule(builder =>
                {
                    builder
                        .WithInterval(Cal.RepeatInterval, Cal.RepeatIntervalUnit)
                        .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(Cal.TimeZoneId))
                        .PreserveHourOfDayAcrossDaylightSavings(Cal.PreserveHourOfDayAcrossDaylightSavings)
                        .SkipDayIfHourDoesNotExist(Cal.SkipDayIfHourDoesNotExist);
                });
            }
            else if (Day != null)
            {
                triggerBuilder = triggerBuilder.WithDailyTimeIntervalSchedule(builder =>
                {
                    builder
                        .WithRepeatCount(Day.RepeatCount)
                        .WithInterval(Day.RepeatInterval, Day.RepeatIntervalUnit)
                        .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(Day.TimeZoneId))
                        .EndingDailyAt(Day.EndTimeOfDay)
                        .StartingDailyAt(Day.StartTimeOfDay)
                        .OnDaysOfTheWeek(Day.DaysOfWeek);
                });
            }

            var trigger = triggerBuilder.Build();

            var returnTrigger = (IOperableTrigger)trigger;
            returnTrigger.SetNextFireTimeUtc(NextFireTimeUtc);
            returnTrigger.SetPreviousFireTimeUtc(PreviousFireTimeUtc);
            returnTrigger.FireInstanceId = FireInstanceId;

            return returnTrigger;
        }

        public void UpdateFireTimes(ITrigger trig)
        {
            NextFireTimeUtc = trig.GetNextFireTimeUtc();
            PreviousFireTimeUtc = trig.GetPreviousFireTimeUtc();
            if (NextFireTimeUtc != null)
            {
                NextFireTimeTicks = NextFireTimeUtc.Value.UtcTicks;
            }
        }
    }



    internal class TriggerComparator : IComparer<Trigger>, IEquatable<TriggerComparator>
    {
        private readonly FireTimeComparator ftc = new FireTimeComparator();

        public int Compare(Trigger trig1, Trigger trig2)
        {
            return ftc.Compare(trig1, trig2);
        }

        public override bool Equals(object obj)
        {
            return obj is TriggerComparator;
        }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(TriggerComparator other)
        {
            return true;
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>
        /// A hash code for the current <see cref="T:System.Object"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return ftc?.GetHashCode() ?? 0;
        }
    }

    public class FireTimeComparator : IComparer<Trigger>
    {
        public int Compare(Trigger trig1, Trigger trig2)
        {
            var t1 = trig1.NextFireTimeUtc;
            var t2 = trig2.NextFireTimeUtc;

            if (t1 != null || t2 != null)
            {
                if (t1 == null)
                {
                    return 1;
                }

                if (t2 == null)
                {
                    return -1;
                }

                if (t1 < t2)
                {
                    return -1;
                }

                if (t1 > t2)
                {
                    return 1;
                }
            }

            var comp = trig2.Priority - trig1.Priority;
            return comp != 0 ? comp : 0;
        }
    }
}
