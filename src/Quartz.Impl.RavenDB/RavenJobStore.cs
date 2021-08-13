using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Configuration;
using System.Data.Common;
using System.Runtime.CompilerServices;
using Quartz.Core;
using Quartz.Spi;
using Quartz.Simpl;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents;

namespace Quartz.Impl.RavenDB
{
    /// <summary> 
    /// An implementation of <see cref="IJobStore" /> to use ravenDB as a persistent Job Store.
    /// Mostly based on RAMJobStore logic with changes to support persistent storage.
    /// Provides an <see cref="IJob" />
    /// and <see cref="ITrigger" /> storage mechanism for the
    /// <see cref="QuartzScheduler" />'s use.
    /// </summary>
    /// <remarks>
    /// Storage of <see cref="IJob" /> s and <see cref="ITrigger" /> s should be keyed
    /// on the combination of their name and group for uniqueness.
    /// </remarks>
    /// <seealso cref="QuartzScheduler" />
    /// <seealso cref="IJobStore" />
    /// <seealso cref="ITrigger" />
    /// <seealso cref="IJob" />
    /// <seealso cref="IJobDetail" />
    /// <seealso cref="JobDataMap" />
    /// <seealso cref="ICalendar" />
    /// <author>Iftah Ben Zaken</author>
    public partial class RavenJobStore : IJobStore
    {
        private TimeSpan misfireThreshold = TimeSpan.FromSeconds(5);
        private ISchedulerSignaler _signaler;
        private static long ftrCtr = SystemTime.UtcNow().Ticks;

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;
        public bool Clustered => false;

        public string InstanceId { get; set; }
        public string InstanceName { get; set; }
        public int ThreadPoolSize { get; set; }

        public static string defaultConnectionString = "Url=http://localhost:8080;DefaultDatabase=MyDatabaseName;ApiKey=YourKey";
        public static string Url { get; set; }
        public static string DefaultDatabase { get; set; }
        public static string ApiKey { get; set; }

        public RavenJobStore()
        {
            var connectionStringSettings = ConfigurationManager.ConnectionStrings["quartznet-ravendb"];
            var stringBuilder = new DbConnectionStringBuilder
            {
                ConnectionString = connectionStringSettings != null ?
                    connectionStringSettings.ConnectionString :
                    defaultConnectionString
            };

            Url = stringBuilder["Url"] as string;
            DefaultDatabase = stringBuilder["DefaultDatabase"] as string;
            ApiKey = stringBuilder.ContainsKey("ApiKey") ? stringBuilder["ApiKey"] as string : null;

            InstanceName = "UnitTestScheduler";
            InstanceId = "instance_two";
        }

        public async Task SetSchedulerState(SchedulerState state, CancellationToken cancellationToken)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                sched.State = state;
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <summary>
        /// Will recover any failed or misfired jobs and clean up the data store as
        /// appropriate.
        /// </summary>
        /// <exception cref="JobPersistenceException">Condition.</exception>
        protected virtual async Task RecoverSchedulerData(CancellationToken cancellationToken)
        {
            try
            {
                // update inconsistent states
                using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                {
                    var queryResult = await session
                        .Query<Trigger>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Acquired || t.State == InternalTriggerState.Blocked))
                        .ToListAsync(cancellationToken);
                    foreach (var trigger in queryResult)
                    {
                        var triggerToUpdate = await session.LoadAsync<Trigger>(trigger.Key, cancellationToken);
                        triggerToUpdate.State = InternalTriggerState.Waiting;
                    }
                    await session.SaveChangesAsync(cancellationToken);
                }

                // recover jobs marked for recovery that were not fully executed
                IList<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                {
                    var queryResultJobs = await session
                        .Query<Job>()
                        .Where(j => (j.Scheduler == InstanceName) && j.RequestsRecovery)
                        .ToListAsync(cancellationToken);

                    foreach (var job in queryResultJobs)
                    {
                        ((List<IOperableTrigger>)recoveringJobTriggers).AddRange(await GetTriggersForJob(new JobKey(job.Name, job.Group), cancellationToken));
                    }
                }
                
                foreach (IOperableTrigger trigger in recoveringJobTriggers)
                {
                    if (await CheckExists(trigger.JobKey))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        await StoreTrigger(trigger, true, cancellationToken);
                    }
                }

                // remove lingering 'complete' triggers...
                IList<Trigger> triggersInStateComplete;

                using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                {
                    triggersInStateComplete = await session
                        .Query<Trigger>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Complete))
                        .ToListAsync(cancellationToken);
                }

                foreach (var trigger in triggersInStateComplete)
                {
                    await RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group), cancellationToken);
                }

                using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                {
                    var schedToUpdate = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                    schedToUpdate.State = SchedulerState.Started;
                    await session.SaveChangesAsync(cancellationToken);
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
            }
        }

        /// <summary>
        /// Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual string GetFiredTriggerRecordId()
        {
            var value = Interlocked.Increment(ref ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Clear (delete!) all scheduling data - all <see cref="IJob"/>s, <see cref="ITrigger" />s
        /// <see cref="ICalendar" />s.
        /// </summary>
        /// <remarks>
        /// </remarks>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ClearAllSchedulingData()
        {
            //var op = DocumentStoreHolder.Store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery(), new BulkOperationOptions() { AllowStale = true });
            //op.WaitForCompletion();
        }

        public async Task<Dictionary<string, ICalendar>> RetrieveCalendarCollection(CancellationToken cancellationToken)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                if (sched is null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }

                if (sched.Calendars is null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Calendar collection in '{0}' is null", InstanceName));
                }

                return sched.Calendars;
            }
        }

        public async Task<ISet<string>> GetPausedJobGroups(CancellationToken cancellationToken)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return (await session.LoadAsync<Scheduler>(InstanceName, cancellationToken)).PausedJobGroups;
            }
        }

        public async Task<ISet<string>> GetBlockedJobs(CancellationToken cancellationToken)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return (await session.LoadAsync<Scheduler>(InstanceName, cancellationToken)).BlockedJobs;
            }
        }

        protected virtual DateTimeOffset MisfireTime
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                DateTimeOffset misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
                }

                return misfireTime;
            }
        }

        protected virtual async Task<bool> ApplyMisfire(Trigger trigger, CancellationToken cancellationToken)
        {
            DateTimeOffset misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            DateTimeOffset? tnft = trigger.NextFireTimeUtc;
            if (!tnft.HasValue || tnft.Value > misfireTime
                || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            await _signaler.NotifyTriggerListenersMisfired(trig, cancellationToken);
            trig.UpdateAfterMisfire(cal);
            trigger.UpdateFireTimes(trig);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await _signaler.NotifySchedulerListenersFinalized(trig, cancellationToken);
                trigger.State = InternalTriggerState.Complete;

            }
            else if (tnft.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        protected virtual async Task SetAllTriggersOfJobToState(JobKey jobKey, InternalTriggerState state, CancellationToken cancellationToken)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var trigs = session.Query<Trigger>()
                    .Where(t => Equals(t.Group, jobKey.Group) && Equals(t.JobName, jobKey.Name));

                foreach (var trig in trigs)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(trig.Key, cancellationToken);
                    triggerToUpdate.State = state;
                }

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        /// <summary> 
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get { return misfireThreshold; }
            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireThreshold must be larger than 0");
                }
                misfireThreshold = value;
            }
        }
    }
}