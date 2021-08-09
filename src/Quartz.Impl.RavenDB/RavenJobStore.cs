using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Configuration;
using System.Data.Common;
using System.Runtime.CompilerServices;

using Common.Logging;

using Quartz.Core;
using Quartz.Impl.Matchers;
using Quartz.Spi;
using Quartz.Simpl;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Quartz.Impl.RavenDB.Util;
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
        private ISchedulerSignaler signaler;
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
        protected ILog Log { get; }

        public RavenJobStore()
        {
            Log = LogManager.GetLogger(GetType());

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

            //
            // TODO: necessary?
            // 
            new TriggerIndex().Execute(DocumentStoreHolder.Store);
            new JobIndex().Execute(DocumentStoreHolder.Store);
        }

        public async Task SetSchedulerState(SchedulerState state, CancellationToken cancellationToken)
        {
            using var session = DocumentStoreHolder.Store.OpenAsyncSession();
            var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
            sched.State = state;
            await session.SaveChangesAsync(cancellationToken);
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
                Log.Info("Trying to recover persisted scheduler data for" + InstanceName);

                // update inconsistent states
                using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                {
                    var queryResult = session.Query<Trigger>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Acquired || t.State == InternalTriggerState.Blocked));
                    foreach (var trigger in queryResult)
                    {
                        var triggerToUpdate = await session.LoadAsync<Trigger>(trigger.Key, cancellationToken);
                        triggerToUpdate.State = InternalTriggerState.Waiting;
                    }
                    await session.SaveChangesAsync(cancellationToken);
                }

                Log.Info("Freed triggers from 'acquired' / 'blocked' state.");
                
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
                        ((List<IOperableTrigger>)recoveringJobTriggers).AddRange(GetTriggersForJob(new JobKey(job.Name, job.Group)));
                    }
                }

                Log.Info("Recovering " + recoveringJobTriggers.Count +
                         " jobs that were in-progress at the time of the last shut-down.");

                foreach (IOperableTrigger trigger in recoveringJobTriggers)
                {
                    if (await CheckExists(trigger.JobKey))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        await StoreTrigger(trigger, true, cancellationToken);
                    }
                }
                Log.Info("Recovery complete.");

                // remove lingering 'complete' triggers...
                Log.Info("Removing 'complete' triggers...");
                IRavenQueryable<Trigger> triggersInStateComplete;

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    triggersInStateComplete = session.Query<Trigger>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Complete));
                }

                foreach (var trigger in triggersInStateComplete)
                {
                    await RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group), cancellationToken);
                }

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var schedToUpdate = session.Load<Scheduler>(InstanceName);
                    schedToUpdate.State = SchedulerState.Started;
                    session.SaveChanges();
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
        /// returns true if the given JobGroup is paused
        /// </summary>
        /// <param name="groupName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool IsJobGroupPaused(string groupName)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                return sched.PausedJobGroups.Contains(groupName);
            }
        }



        /// <summary>
        /// Retrieve the <see cref="IJobDetail" /> for the given
        /// <see cref="IJob" />.
        /// </summary>
        /// <returns>
        /// The desired <see cref="IJob" />, or null if there is no match.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IJobDetail RetrieveJob(JobKey jobKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var job = session.Load<Job>(jobKey.Name + "/" + jobKey.Group);

                return job?.Deserialize();
            }
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
            using var session = DocumentStoreHolder.Store.OpenAsyncSession();

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

        /// <summary>
        /// Get the number of <see cref="IJob" />s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public int GetNumberOfJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Job>().Count();
            }
        }

        /// <summary>
        /// Get the number of <see cref="ITrigger" />s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public int GetNumberOfTriggers()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Trigger>().Count();
            }
        }


        /// <summary>
        /// Get the names of all of the <see cref="IJob" /> s that
        /// have the given group name.
        /// <para>
        /// If there are no jobs in the given group name, the result should be a
        /// zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        /// <param name="matcher"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<JobKey>();

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var allJobs = session.Query<Job>();

                foreach (var job in allJobs)
                {
                    if (op.Evaluate(job.Group, compareToValue))
                    {
                        result.Add(new JobKey(job.Name, job.Group));
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Get the names of all of the <see cref="ITrigger" />s
        /// that have the given group name.
        /// <para>
        /// If there are no triggers in the given group name, the result should be a
        /// zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<TriggerKey>();

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var allTriggers = session.Query<Trigger>();

                foreach (var trigger in allTriggers)
                {
                    if (op.Evaluate(trigger.Group, compareToValue))
                    {
                        result.Add(new TriggerKey(trigger.Name, trigger.Group));
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Get the names of all of the <see cref="IJob" />
        /// groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        /// array (not <see langword="null" />).
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<string> GetJobGroupNames()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Query<Job>()
                    .Select(j => j.Group)
                    .Distinct()
                    .ToList();
            }
        }

        /// <summary>
        /// Get the names of all of the <see cref="ITrigger" />
        /// groups.
        /// <para>
        /// If there are no known group names, the result should be a zero-length
        /// array (not <see langword="null" />).
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<string> GetTriggerGroupNames()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                try
                {
                    var result = session.Query<Trigger>()
                        .Select(t => t.Group)
                        .Distinct()
                        .ToList();
                    return result;
                }
                catch (ArgumentNullException)
                {
                    return new List<string>();
                }
            }
        }

        /// <summary>
        /// Get all of the Triggers that are associated to the given Job.
        /// </summary>
        /// <remarks>
        /// If there are no matches, a zero-length array should be returned.
        /// </remarks>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<IOperableTrigger> GetTriggersForJob(JobKey jobKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                try
                {
                    var result = session
                        .Query<Trigger>()
                        .Where(t => Equals(t.JobName, jobKey.Name) && Equals(t.Group, jobKey.Group))
                        .ToList()
                        .Select(trigger => trigger.Deserialize()).ToList();
                    return result;
                }
                catch(NullReferenceException)
                {
                    return new List<IOperableTrigger>();
                }
            }
        }

        /// <summary>
        /// Get the current state of the identified <see cref="ITrigger" />.
        /// </summary>
        /// <seealso cref="TriggerState" />
        [MethodImpl(MethodImplOptions.Synchronized)]
        public TriggerState GetTriggerState(TriggerKey triggerKey)
        {
            Trigger trigger;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                trigger = session.Load<Trigger>(triggerKey.GetDatabaseId());
            }

            if (trigger == null)
            {
                return TriggerState.None;
            }

            switch (trigger.State)
            {
                case InternalTriggerState.Complete:
                    return TriggerState.Complete;
                case InternalTriggerState.Paused:
                    return TriggerState.Paused;
                case InternalTriggerState.PausedAndBlocked:
                    return TriggerState.Paused;
                case InternalTriggerState.Blocked:
                    return TriggerState.Blocked;
                case InternalTriggerState.Error:
                    return TriggerState.Error;
                default:
                    return TriggerState.Normal;
            }
        }



        /// <summary>
        /// Gets the paused job groups.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ISet<string> GetPausedJobGroups()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<Scheduler>(InstanceName).PausedJobGroups;
            }
        }

        /// <summary>
        /// Gets the blocked jobs set.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ISet<string> GetBlockedJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<Scheduler>(InstanceName).BlockedJobs;
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
            await signaler.NotifyTriggerListenersMisfired(trig, cancellationToken);
            trig.UpdateAfterMisfire(cal);
            trigger.UpdateFireTimes(trig);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await signaler.NotifySchedulerListenersFinalized(trig, cancellationToken);
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
            using var session = DocumentStoreHolder.Store.OpenAsyncSession();
            
            var trigs = session.Query<Trigger>()
                .Where(t => Equals(t.Group, jobKey.Group) && Equals(t.JobName, jobKey.Name));

            foreach (var trig in trigs)
            {
                var triggerToUpdate = await session.LoadAsync<Trigger>(trig.Key, cancellationToken);
                triggerToUpdate.State = state;
            }

            await session.SaveChangesAsync(cancellationToken);
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