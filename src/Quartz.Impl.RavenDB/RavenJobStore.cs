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

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Linq;

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
    public class RavenJobStore : IJobStore
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

        public static string defaultConnectionString = "Url=http://localhost:8080;DefaultDatabase=MyDatabaseName";
        public static string Url { get; set; }
        public static string DefaultDatabase { get; set; }

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
         
            InstanceName = "UnitTestScheduler";
            InstanceId = "instance_two";


            new TriggerIndex().Execute(DocumentStoreHolder.Store);
            new JobIndex().Execute(DocumentStoreHolder.Store);
        }

        /// <summary>
        /// Called by the QuartzScheduler before the <see cref="IJobStore" /> is
        /// used, in order to give it a chance to Initialize.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler s)
        {
            signaler = s;
        }

        /// <summary>
        /// Sets the schedulers's state
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SetSchedulerState(string state)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                sched.State = state;
                session.SaveChanges();
            }
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// the scheduler has started.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SchedulerStarted()
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(InstanceName);
            if (docMetaData != null)
            {
                // Scheduler with same instance name already exists, recover persistent data
                try
                {
                    RecoverSchedulerData();
                }
                catch (SchedulerException se)
                {
                    throw new SchedulerConfigException("Failure occurred during job recovery.", se);
                }
                return;
            }

            // If schefuler doesn't exist create new empty scheduler and store it
            var schedToStore = new Scheduler
            {
                InstanceName = InstanceName,
                LastCheckinTime = DateTimeOffset.MinValue,
                CheckinInterval = DateTimeOffset.MinValue,
                Calendars = new Dictionary<string, ICalendar>(),
                PausedJobGroups = new Collection.HashSet<string>(),
                BlockedJobs = new Collection.HashSet<string>(),
                State = "Started"
            };

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                session.Store(schedToStore, InstanceName);
                session.SaveChanges();
            }            
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has been paused.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SchedulerPaused()
        {
            SetSchedulerState("Paused");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the JobStore that
        /// the scheduler has resumed after being paused.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SchedulerResumed()
        {
            SetSchedulerState("Resumed");
        }

        /// <summary>
        /// Called by the QuartzScheduler to inform the <see cref="IJobStore" /> that
        /// it should free up all of it's resources because the scheduler is
        /// shutting down.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Shutdown()
        {
            SetSchedulerState("Shutdown");
        }

        /// <summary>
        /// Will recover any failed or misfired jobs and clean up the data store as
        /// appropriate.
        /// </summary>
        /// <exception cref="JobPersistenceException">Condition.</exception>
        [MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual void RecoverSchedulerData()
        {
            try
            {
                Log.Info("Trying to recover persisted scheduler data for" + InstanceName);

                // update inconsistent states
                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var queryResult = session.Query<Trigger>()
                        .Where(t => (t.Scheduler == InstanceName) && (t.State == InternalTriggerState.Acquired || t.State == InternalTriggerState.Blocked));
                    foreach (var trigger in queryResult)
                    {
                        var triggerToUpdate = session.Load<Trigger>(trigger.Key);
                        triggerToUpdate.State = InternalTriggerState.Waiting;
                    }
                    session.SaveChanges();
                }

                Log.Info("Freed triggers from 'acquired' / 'blocked' state.");
                
                // recover jobs marked for recovery that were not fully executed
                IList<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var queryResultJobs = session.Query<Job>()
                        .Where(j => (j.Scheduler == InstanceName) && j.RequestsRecovery);

                    foreach (var job in queryResultJobs)
                    {
                        recoveringJobTriggers.AddRange(GetTriggersForJob(new JobKey(job.Name, job.Group)));
                    }
                }

                Log.Info("Recovering " + recoveringJobTriggers.Count +
                         " jobs that were in-progress at the time of the last shut-down.");

                foreach (IOperableTrigger trigger in recoveringJobTriggers)
                {
                    if (CheckExists(trigger.JobKey))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        StoreTrigger(trigger, true);
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
                    RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group));
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
        /// Store the given <see cref="IJobDetail" /> and <see cref="ITrigger" />.
        /// </summary>
        /// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
        /// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger)
        {
            StoreJob(newJob, true);
            StoreTrigger(newTrigger, true);
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
        /// returns true if the given TriggerGroup
        /// is paused
        /// </summary>
        /// <param name="groupName"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool IsTriggerGroupPaused(string groupName)
        {
            return GetPausedTriggerGroups().Contains(groupName);

        }

        /// <summary>
        /// Store the given <see cref="IJobDetail" />.
        /// </summary>
        /// <param name="newJob">The <see cref="IJobDetail" /> to be stored.</param>
        /// <param name="replaceExisting">
        /// If <see langword="true" />, any <see cref="IJob" /> existing in the
        /// <see cref="IJobStore" /> with the same name and group should be
        /// over-written.
        /// </param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void StoreJob(IJobDetail newJob, bool replaceExisting)
        {
            if (CheckExists(newJob.Key))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }
            }

            var job = new Job(newJob, InstanceName);

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                // Store() overwrites if job id already exists
                session.Store(job, job.Key);
                session.SaveChanges();
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void StoreJobsAndTriggers(IDictionary<IJobDetail, Collection.ISet<ITrigger>> triggersAndJobs, bool replace)
        {
            using (var bulkInsert = DocumentStoreHolder.Store.BulkInsert(options: new BulkInsertOptions() { OverwriteExisting = replace }))
            {
                foreach (var pair in triggersAndJobs)
                {
                    // First store the current job
                    bulkInsert.Store(new Job(pair.Key, InstanceName), pair.Key.Key.Name + "/" + pair.Key.Key.Group);
                    
                    // Storing all triggers for the current job
                    foreach (var trig in pair.Value)
                    {
                        var operTrig = trig as IOperableTrigger;
                        if (operTrig == null)
                        {
                            continue;
                        }
                        var trigger = new Trigger(operTrig, InstanceName);

                        if (GetPausedTriggerGroups().Contains(operTrig.Key.Group) || GetPausedJobGroups().Contains(operTrig.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Paused;
                            if (GetBlockedJobs().Contains(operTrig.JobKey.Name + "/" + operTrig.JobKey.Group))
                            {
                                trigger.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        else if (GetBlockedJobs().Contains(operTrig.JobKey.Name + "/" + operTrig.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Blocked;
                        }

                        bulkInsert.Store(trigger, trigger.Key);
                    }
                }
            } // bulkInsert is disposed - same effect as session.SaveChanges()

        }

        /// <summary>
        /// Remove (delete) the <see cref="IJob" /> with the given
        /// key, and any <see cref="ITrigger" /> s that reference
        /// it.
        /// </summary>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="IJob" /> with the given name and
        /// group was found and removed from the store.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool RemoveJob(JobKey jobKey)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                if (!CheckExists(jobKey))
                {
                    return false;
                }

                session.Advanced.Defer(new DeleteCommandData
                {
                    Key = jobKey.Name + "/" + jobKey.Group
                });
                session.SaveChanges();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool RemoveJobs(IList<JobKey> jobKeys)
        {
            // Returns false in case at least one job removal fails
            var result = true;
            foreach (var key in jobKeys)
            {
                result &= RemoveJob(key);
            }
            return result;
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
        /// Store the given <see cref="ITrigger" />.
        /// </summary>
        /// <param name="newTrigger">The <see cref="ITrigger" /> to be stored.</param>
        /// <param name="replaceExisting">If <see langword="true" />, any <see cref="ITrigger" /> existing in
        /// the <see cref="IJobStore" /> with the same name and group should
        /// be over-written.</param>
        /// <throws>  ObjectAlreadyExistsException </throws>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting)
        {
            if (CheckExists(newTrigger.Key))
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newTrigger);
                }
            }

            if (!CheckExists(newTrigger.JobKey))
            {
                throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
            }

            var trigger = new Trigger(newTrigger, InstanceName);

            // make sure trigger group is not paused and that job is not blocked
            if (GetPausedTriggerGroups().Contains(newTrigger.Key.Group) || GetPausedJobGroups().Contains(newTrigger.JobKey.Group))
            {
                trigger.State = InternalTriggerState.Paused;
                if (GetBlockedJobs().Contains(newTrigger.JobKey.Name + "/" + newTrigger.JobKey.Group))
                {
                    trigger.State = InternalTriggerState.PausedAndBlocked;
                }
            }
            else if (GetBlockedJobs().Contains(newTrigger.JobKey.Name + "/" + newTrigger.JobKey.Group))
            {
                trigger.State = InternalTriggerState.Blocked;
            }

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                // Overwrite if exists
                session.Store(trigger, trigger.Key);
                session.SaveChanges();
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ITrigger" /> with the given key.
        /// </summary>
        /// <remarks>
        /// <para>
        /// If removal of the <see cref="ITrigger" /> results in an 'orphaned' <see cref="IJob" />
        /// that is not 'durable', then the <see cref="IJob" /> should be deleted
        /// also.
        /// </para>
        /// </remarks>
        /// <returns>
        /// <see langword="true" /> if a <see cref="ITrigger" /> with the given
        /// name and group was found and removed from the store.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool RemoveTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey))
            {
                return false;
            }
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
                var job = RetrieveJob(new JobKey(trigger.JobName, trigger.Group));

                // Delete trigger
                session.Advanced.Defer(new DeleteCommandData
                {
                    Key = triggerKey.Name + "/" + triggerKey.Group
                });
                session.SaveChanges();

                // Remove the trigger's job if it is not associated with any other triggers
                var trigList = GetTriggersForJob(job.Key);
                if ((trigList == null || trigList.Count == 0) && !job.Durable)
                {
                    if (RemoveJob(job.Key))
                    {
                        signaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool RemoveTriggers(IList<TriggerKey> triggerKeys)
        {
            // Returns false in case at least one trigger removal fails
            var result = true;
            foreach (var key in triggerKeys)
            {
                result &= RemoveTrigger(key);
            }
            return result;
        }

        /// <summary>
        /// Remove (delete) the <see cref="ITrigger" /> with the
        /// given name, and store the new given one - which must be associated
        /// with the same job.
        /// </summary>
        /// <param name="triggerKey">The <see cref="ITrigger"/> to be replaced.</param>
        /// <param name="newTrigger">The new <see cref="ITrigger" /> to be stored.</param>
        /// <returns>
        /// 	<see langword="true" /> if a <see cref="ITrigger" /> with the given
        /// name and group was found and removed from the store.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger)
        {
            if (!CheckExists(triggerKey))
            {
                return false;
            }
            var wasRemoved = RemoveTrigger(triggerKey);
            if (wasRemoved)
            {
                StoreTrigger(newTrigger, true);
            }
            return wasRemoved;
        }

        /// <summary>
        /// Retrieve the given <see cref="ITrigger" />.
        /// </summary>
        /// <returns>
        /// The desired <see cref="ITrigger" />, or null if there is no
        /// match.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IOperableTrigger RetrieveTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey))
            {
                return null;
            }

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                return trigger?.Deserialize();
            }
        }

        /// <summary>
        /// Determine whether a <see cref="ICalendar" /> with the given identifier already
        /// exists within the scheduler.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="calName">the identifier to check for</param>
        /// <returns>true if a calendar exists with the given identifier</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool CalendarExists(string calName)
        {
            bool answer;
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                if (sched == null) return false;
                try
                {
                    answer = sched.Calendars.ContainsKey(calName);
                }
                catch (ArgumentNullException argumentNullException)
                {
                    Log.Error("Calendars collection is null.", argumentNullException);
                    answer = false;
                }
            }
            return answer;
        }

        /// <summary>
        /// Determine whether a <see cref="IJob" /> with the given identifier already
        /// exists within the scheduler.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="jobKey">the identifier to check for</param>
        /// <returns>true if a job exists with the given identifier</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool CheckExists(JobKey jobKey)
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(jobKey.Name + "/" + jobKey.Group);
            return docMetaData != null;
        }

        /// <summary>
        /// Determine whether a <see cref="ITrigger" /> with the given identifier already
        /// exists within the scheduler.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="triggerKey">the identifier to check for</param>
        /// <returns>true if a trigger exists with the given identifier</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool CheckExists(TriggerKey triggerKey)
        {
            var cmds = DocumentStoreHolder.Store.DatabaseCommands;
            var docMetaData = cmds.Head(triggerKey.Name + "/" + triggerKey.Group);
            return docMetaData != null;
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
            var op = DocumentStoreHolder.Store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery(), new BulkOperationOptions() { AllowStale = true });
            op.WaitForCompletion();
        }

        /// <summary>
        /// Store the <see cref="ICalendar" /> with the given identifier.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <param name="name">the identifier for the calendar</param>
        /// <param name="calendar">the name of the calendar</param>
        /// <param name="replaceExisting">should replace existing calendar</param>
        /// <param name="updateTriggers">should update triggers</param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers)
        {
            var calendarCopy = (ICalendar)calendar.Clone();

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);

                if (sched?.Calendars == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }

                if (CalendarExists(name) && !replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture, "Calendar with name '{0}' already exists.", name));
                }

                // add or replace calendar
                sched.Calendars[name] = calendarCopy;

                if (!updateTriggers)
                {
                    return;
                }

                var triggersKeysToUpdate = session
                    .Query<Trigger>()
                    .Where(t => t.CalendarName == name)
                    .Select(t => t.Key)
                    .ToList();

                if (triggersKeysToUpdate.Count == 0)
                {
                    session.SaveChanges();
                    return;
                }

                foreach (var triggerKey in triggersKeysToUpdate)
                {
                    var triggerToUpdate = session.Load<Trigger>(triggerKey);
                    var trigger = triggerToUpdate.Deserialize();
                    trigger.UpdateWithNewCalendar(calendarCopy, misfireThreshold);
                    triggerToUpdate.UpdateFireTimes(trigger);

                }
                session.SaveChanges();
            }
        }

        /// <summary>
        /// Remove (delete) the <see cref="ICalendar" /> with the
        /// given name.
        /// </summary>
        /// <remarks>
        /// If removal of the <see cref="ICalendar" /> would result in
        /// <see cref="ITrigger" />s pointing to non-existent calendars, then a
        /// <see cref="JobPersistenceException" /> will be thrown.
        /// </remarks>
        /// <param name="calName">The name of the <see cref="ICalendar" /> to be removed.</param>
        /// <returns>
        /// <see langword="true" /> if a <see cref="ICalendar" /> with the given name
        /// was found and removed from the store.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool RemoveCalendar(string calName)
        {
            if (RetrieveCalendar(calName) == null)
            {
                return false;
            }
            var calCollection = RetrieveCalendarCollection();
            calCollection.Remove(calName);

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                sched.Calendars = calCollection;
                session.SaveChanges();
            }
            return true;
        }

        /// <summary>
        /// Retrieve the given <see cref="ITrigger" />.
        /// </summary>
        /// <param name="calName">The name of the <see cref="ICalendar" /> to be retrieved.</param>
        /// <returns>
        /// The desired <see cref="ICalendar" />, or null if there is no
        /// match.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ICalendar RetrieveCalendar(string calName)
        {
            var callCollection = RetrieveCalendarCollection();
            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        /// <summary>
        /// Get the <see cref="ICalendar" />s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Dictionary<string, ICalendar> RetrieveCalendarCollection()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);
                if (sched == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }
                if (sched.Calendars == null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Calendar collection in '{0}' is null", InstanceName));
                }
                return sched.Calendars;
            }
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
        /// Get the number of <see cref="ICalendar" /> s that are
        /// stored in the <see cref="IJobStore" />.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public int GetNumberOfCalendars()
        {
            return RetrieveCalendarCollection().Count;
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
        public Collection.ISet<JobKey> GetJobKeys(GroupMatcher<JobKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new Collection.HashSet<JobKey>();

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
        public Collection.ISet<TriggerKey> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new Collection.HashSet<TriggerKey>();

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
        /// Get the names of all of the <see cref="ICalendar" /> s
        /// in the <see cref="IJobStore" />.
        /// <para>
        /// If there are no Calendars in the given group name, the result should be
        /// a zero-length array (not <see langword="null" />).
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<string> GetCalendarNames()
        {
            return RetrieveCalendarCollection().Keys.ToList();
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
                trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);
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
        /// Pause the <see cref="ITrigger" /> with the given key.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void PauseTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey))
            {
                return;
            }

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trig = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger doesn't exist or is "complete" pausing it does not make sense...
                if (trig == null)
                {
                    return;
                }
                if (trig.State == InternalTriggerState.Complete)
                {
                    return;
                }

                trig.State = trig.State == InternalTriggerState.Blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused;
                session.SaveChanges();
            }
        }

        /// <summary>
        /// Pause all of the <see cref="ITrigger" />s in the
        /// given group.
        /// </summary>
        /// <remarks>
        /// The JobStore should "remember" that the group is paused, and impose the
        /// pause on any new triggers that are added to the group while the group is
        /// paused.
        /// </remarks>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection.ISet<string> PauseTriggers(GroupMatcher<TriggerKey> matcher)
        {
            var pausedGroups = new HashSet<string>();

            var triggerKeysForMatchedGroup = GetTriggerKeys(matcher);
            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                PauseTrigger(triggerKey);
                pausedGroups.Add(triggerKey.Group);
            }
            return new Collection.HashSet<string>(pausedGroups);
        }

        /// <summary>
        /// Pause the <see cref="IJob" /> with the given key - by
        /// pausing all of its current <see cref="ITrigger" />s.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void PauseJob(JobKey jobKey)
        {
            IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                PauseTrigger(trigger.Key);
            }
        }

        /// <summary>
        /// Pause all of the <see cref="IJob" />s in the given
        /// group - by pausing all of their <see cref="ITrigger" />s.
        /// <para>
        /// The JobStore should "remember" that the group is paused, and impose the
        /// pause on any new jobs that are added to the group while the group is
        /// paused.
        /// </para>
        /// </summary>
        /// <seealso cref="string">
        /// </seealso>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<string> PauseJobs(GroupMatcher<JobKey> matcher)
        {

            var pausedGroups = new List<string>();

            var jobKeysForMatchedGroup = GetJobKeys(matcher);
            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                PauseJob(jobKey);
                pausedGroups.Add(jobKey.Group);

                using (var session = DocumentStoreHolder.Store.OpenSession())
                {
                    var sched = session.Load<Scheduler>(InstanceName);
                    sched.PausedJobGroups.Add(matcher.CompareToValue);
                    session.SaveChanges();
                }
            }

            return pausedGroups;
        }

        /// <summary>
        /// Resume (un-pause) the <see cref="ITrigger" /> with the
        /// given key.
        /// 
        /// <para>
        /// If the <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="string">
        /// </seealso>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ResumeTrigger(TriggerKey triggerKey)
        {
            if (!CheckExists(triggerKey))
            {
                return;
            }
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(triggerKey.Name + "/" + triggerKey.Group);

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused &&
                    trigger.State != InternalTriggerState.PausedAndBlocked)
                {
                    return;
                }

                trigger.State = GetBlockedJobs().Contains(trigger.JobKey) ? InternalTriggerState.Blocked : InternalTriggerState.Waiting;

                ApplyMisfire(trigger);

                session.SaveChanges();
            }
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="ITrigger" />s
        /// in the given group.
        /// <para>
        /// If any <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<string> ResumeTriggers(GroupMatcher<TriggerKey> matcher)
        {
            Collection.ISet<string> resumedGroups = new Collection.HashSet<string>();
            Collection.ISet<TriggerKey> keys = GetTriggerKeys(matcher);

            foreach (TriggerKey triggerKey in keys)
            {
                ResumeTrigger(triggerKey);
                resumedGroups.Add(triggerKey.Group);
            }

            return new List<string>(resumedGroups);
        }

        /// <summary>
        /// Gets the paused trigger groups.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection.ISet<string> GetPausedTriggerGroups()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return new Collection.HashSet<string>(
                    session.Query<Trigger>()
                        .Where(t => t.State == InternalTriggerState.Paused || t.State == InternalTriggerState.PausedAndBlocked)
                        .Distinct()
                        .Select(t => t.Group)
                        .ToHashSet()
                );
            }
        }

        /// <summary>
        /// Gets the paused job groups.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection.ISet<string> GetPausedJobGroups()
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
        public Collection.ISet<string> GetBlockedJobs()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                return session.Load<Scheduler>(InstanceName).BlockedJobs;
            }
        }

        /// <summary> 
        /// Resume (un-pause) the <see cref="IJob" /> with the
        /// given key.
        /// <para>
        /// If any of the <see cref="IJob" />'s<see cref="ITrigger" /> s missed one
        /// or more fire-times, then the <see cref="ITrigger" />'s misfire
        /// instruction will be applied.
        /// </para>
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ResumeJob(JobKey jobKey)
        {
            IList<IOperableTrigger> triggersForJob = GetTriggersForJob(jobKey);
            foreach (IOperableTrigger trigger in triggersForJob)
            {
                ResumeTrigger(trigger.Key);
            }
        }

        /// <summary>
        /// Resume (un-pause) all of the <see cref="IJob" />s in
        /// the given group.
        /// <para>
        /// If any of the <see cref="IJob" /> s had <see cref="ITrigger" /> s that
        /// missed one or more fire-times, then the <see cref="ITrigger" />'s
        /// misfire instruction will be applied.
        /// </para> 
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Collection.ISet<string> ResumeJobs(GroupMatcher<JobKey> matcher)
        {
            Collection.ISet<string> resumedGroups = new Collection.HashSet<string>();

            Collection.ISet<JobKey> keys = GetJobKeys(matcher);

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);

                foreach (var pausedJobGroup in sched.PausedJobGroups)
                {
                    if (matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue))
                    {
                        resumedGroups.Add(pausedJobGroup);
                    }
                }

                foreach (var resumedGroup in resumedGroups)
                {
                    sched.PausedJobGroups.Remove(resumedGroup);
                }
                session.SaveChanges();
            }

            foreach (JobKey key in keys)
            {
                IList<IOperableTrigger> triggers = GetTriggersForJob(key);
                foreach (IOperableTrigger trigger in triggers)
                {
                    ResumeTrigger(trigger.Key);
                }
            }

            return resumedGroups;
        }

        /// <summary>
        /// Pause all triggers - equivalent of calling <see cref="PauseTriggers" />
        /// on every group.
        /// <para>
        /// When <see cref="ResumeAll" /> is called (to un-pause), trigger misfire
        /// instructions WILL be applied.
        /// </para>
        /// </summary>
        /// <seealso cref="ResumeAll" />
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void PauseAll()
        {
            IList<string> triggerGroupNames = GetTriggerGroupNames();

            foreach (var groupName in triggerGroupNames)
            {
                PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
            }
        }

        /// <summary>
        /// Resume (un-pause) all triggers - equivalent of calling <see cref="ResumeTriggers" />
        /// on every group.
        /// <para>
        /// If any <see cref="ITrigger" /> missed one or more fire-times, then the
        /// <see cref="ITrigger" />'s misfire instruction will be applied.
        /// </para>
        /// 
        /// </summary>
        /// <seealso cref="PauseAll" />
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ResumeAll()
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var sched = session.Load<Scheduler>(InstanceName);

                sched.PausedJobGroups.Clear();

                var triggerGroupNames = GetTriggerGroupNames();

                foreach (var groupName in triggerGroupNames)
                {
                    ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));
                }
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

        /// <summary>
        /// Applies the misfire.
        /// </summary>
        /// <param name="trigger">The trigger wrapper.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual bool ApplyMisfire(Trigger trigger)
        {
            DateTimeOffset? tnft = trigger.NextFireTimeUtc;
            if (!tnft.HasValue || tnft.Value > MisfireTime
                || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
            {
                return false;
            }

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = RetrieveCalendar(trigger.CalendarName);
            }

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            signaler.NotifyTriggerListenersMisfired(trig);
            trig.UpdateAfterMisfire(cal);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                signaler.NotifySchedulerListenersFinalized(trig);
                trigger.UpdateFireTimes(trig);
                trigger.State = InternalTriggerState.Complete;

            }
            else if (tnft.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Get a handle to the next trigger to be fired, and mark it as 'reserved'
        /// by the calling scheduler.
        /// </summary>
        /// <param name="noLaterThan">If &gt; 0, the JobStore should only return a Trigger
        /// that will fire no later than the time represented in this value as
        /// milliseconds.</param>
        /// <param name="maxCount"></param>
        /// <param name="timeWindow"></param>
        /// <returns></returns>
        /// <seealso cref="ITrigger">
        /// </seealso>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public virtual IList<IOperableTrigger> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow)
        {
            List<IOperableTrigger> result = new List<IOperableTrigger>();
            Collection.ISet<JobKey> acquiredJobKeysForNoConcurrentExec = new Collection.HashSet<JobKey>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var triggersQuery = session.Query<Trigger>()
                    .Where(t => (t.State == InternalTriggerState.Waiting) && (t.NextFireTimeUtc <= (noLaterThan + timeWindow).UtcDateTime) &&
                                ((t.MisfireInstruction == -1) || ((t.MisfireInstruction != -1) && (t.NextFireTimeUtc >= MisfireTime))))
                    .OrderBy(t => t.NextFireTimeTicks)
                    .ThenByDescending(t => t.Priority)
                    .ToList();

                var triggers = new SortedSet<Trigger>(triggersQuery, new TriggerComparator());

                while (true)
                {
                    // return empty list if store has no such triggers.
                    if (!triggers.Any())
                    {
                        return result;
                    }

                    var candidateTrigger = triggers.First();
                    if (candidateTrigger == null)
                    {
                        break;
                    }
                    if (!triggers.Remove(candidateTrigger))
                    {
                        break;
                    }
                    if (candidateTrigger.NextFireTimeUtc == null)
                    {
                        continue;
                    }

                    if (ApplyMisfire(candidateTrigger))
                    {
                        if (candidateTrigger.NextFireTimeUtc != null)
                        {
                            triggers.Add(candidateTrigger);
                        }
                        continue;
                    }

                    if (candidateTrigger.NextFireTimeUtc > noLaterThan + timeWindow)
                    {
                        break;
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = new JobKey(candidateTrigger.JobName, candidateTrigger.Group);
                    Job job = session.Load<Job>(candidateTrigger.JobKey);

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue; // go to next trigger in store.
                        }
                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }

                    candidateTrigger.State = InternalTriggerState.Acquired;
                    candidateTrigger.FireInstanceId = GetFiredTriggerRecordId();

                    result.Add(candidateTrigger.Deserialize());

                    if (firstAcquiredTriggerFireTime == null)
                    {
                        firstAcquiredTriggerFireTime = candidateTrigger.NextFireTimeUtc;
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }
                session.SaveChanges();
            }
            return result;
        }

        /// <summary> 
        /// Inform the <see cref="IJobStore" /> that the scheduler no longer plans to
        /// fire the given <see cref="ITrigger" />, that it had previously acquired
        /// (reserved).
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ReleaseAcquiredTrigger(IOperableTrigger trig)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(trig.Key.Name + "/" + trig.Key.Group);
                if ((trigger == null) || (trigger.State != InternalTriggerState.Acquired))
                {
                    return;
                }
                trigger.State = InternalTriggerState.Waiting;
                session.SaveChanges();
            }
        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> that the scheduler is now firing the
        /// given <see cref="ITrigger" /> (executing its associated <see cref="IJob" />),
        /// that it had previously acquired (reserved).
        /// </summary>
        /// <returns>
        /// May return null if all the triggers or their calendars no longer exist, or
        /// if the trigger was not successfully put into the 'executing'
        /// state.  Preference is to return an empty list if none of the triggers
        /// could be fired.
        /// </returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public IList<TriggerFiredResult> TriggersFired(IList<IOperableTrigger> triggers)
        {
            List<TriggerFiredResult> results = new List<TriggerFiredResult>();
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                foreach (IOperableTrigger tr in triggers)
                {
                    // was the trigger deleted since being acquired?
                    var trigger = session.Load<Trigger>(tr.Key.Name + "/" + tr.Key.Group);

                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (trigger?.State != InternalTriggerState.Acquired)
                    {
                        continue;
                    }

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = RetrieveCalendar(trigger.CalendarName);
                        if (cal == null)
                        {
                            continue;
                        }
                    }
                    DateTimeOffset? prevFireTime = trigger.PreviousFireTimeUtc;

                    var trig = trigger.Deserialize();
                    trig.Triggered(cal);

                    TriggerFiredBundle bndle = new TriggerFiredBundle(RetrieveJob(trig.JobKey),
                        trig,
                        cal,
                        false, SystemTime.UtcNow(),
                        trig.GetPreviousFireTimeUtc(), prevFireTime,
                        trig.GetNextFireTimeUtc());

                    IJobDetail job = bndle.JobDetail;

                    trigger.UpdateFireTimes(trig);
                    trigger.State = InternalTriggerState.Waiting;

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        var trigs = session.Query<Trigger>()
                            .Where(t => Equals(t.Group, job.Key.Group) && Equals(t.JobName, job.Key.Name));

                        foreach (var t in trigs)
                        {
                            if (t.State == InternalTriggerState.Waiting)
                            {
                                t.State = InternalTriggerState.Blocked;
                            }
                            if (t.State == InternalTriggerState.Paused)
                            {
                                t.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        var sched = session.Load<Scheduler>(InstanceName);
                        sched.BlockedJobs.Add(job.Key.Name + "/" + job.Key.Group);
                    }

                    results.Add(new TriggerFiredResult(bndle));
                }
                session.SaveChanges();
            }
            return results;

        }

        /// <summary>
        /// Inform the <see cref="IJobStore" /> that the scheduler has completed the
        /// firing of the given <see cref="ITrigger" /> (and the execution its
        /// associated <see cref="IJob" />), and that the <see cref="JobDataMap" />
        /// in the given <see cref="IJobDetail" /> should be updated if the <see cref="IJob" />
        /// is stateful.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void TriggeredJobComplete(IOperableTrigger trig, IJobDetail jobDetail, SchedulerInstruction triggerInstCode)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigger = session.Load<Trigger>(trig.Key.Name + "/" + trig.Key.Group);
                var sched = session.Load<Scheduler>(InstanceName);

                // It's possible that the job or trigger is null if it was deleted during execution
                var job = session.Load<Job>(trig.JobKey.Name + "/" + trig.JobKey.Group);

                if (job != null)
                {
                    if (jobDetail.PersistJobDataAfterExecution)
                    {
                        job.JobDataMap = jobDetail.JobDataMap;

                    }
                    if (job.ConcurrentExecutionDisallowed)
                    {
                        sched.BlockedJobs.Remove(job.Key);

                        List<Trigger> trigs = session.Query<Trigger>()
                            .Where(t => Equals(t.Group, job.Group) && Equals(t.JobName, job.Name))
                            .ToList();

                        foreach (Trigger t in trigs)
                        {
                            var triggerToUpdate = session.Load<Trigger>(t.Key);
                            if (t.State == InternalTriggerState.Blocked)
                            {
                                triggerToUpdate.State = InternalTriggerState.Waiting;
                            }
                            if (t.State == InternalTriggerState.PausedAndBlocked)
                            {
                                triggerToUpdate.State = InternalTriggerState.Paused;
                            }
                        }

                        signaler.SignalSchedulingChange(null);
                    }
                }
                else
                {
                    // even if it was deleted, there may be cleanup to do
                    sched.BlockedJobs.Remove(jobDetail.Key.Name + "/" + jobDetail.Key.Group);
                }

                // check for trigger deleted during execution...
                if (trigger != null)
                {
                    if (triggerInstCode == SchedulerInstruction.DeleteTrigger)
                    {
                        // Deleting triggers
                        DateTimeOffset? d = trig.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = trigger.NextFireTimeUtc;
                            if (!d.HasValue)
                            {
                                RemoveTrigger(trig.Key);
                            }
                            else
                            {
                                //Deleting cancelled - trigger still active
                            }
                        }
                        else
                        {
                            RemoveTrigger(trig.Key);
                            signaler.SignalSchedulingChange(null);
                        }
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                    {
                        trigger.State = InternalTriggerState.Complete;
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                    {
                        trigger.State = InternalTriggerState.Error;
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                    {
                        SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Error);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                    {
                        SetAllTriggersOfJobToState(trig.JobKey, InternalTriggerState.Complete);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                session.SaveChanges();
            }
        }


        /// <summary>
        /// Sets the State of all triggers of job to specified State.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        protected virtual void SetAllTriggersOfJobToState(JobKey jobKey, InternalTriggerState state)
        {
            using (var session = DocumentStoreHolder.Store.OpenSession())
            {
                var trigs = session.Query<Trigger>()
                    .Where(t => Equals(t.Group, jobKey.Group) && Equals(t.JobName, jobKey.Name));

                foreach (var trig in trigs)
                {
                    var triggerToUpdate = session.Load<Trigger>(trig.Key);
                    triggerToUpdate.State = state;
                }
                session.SaveChanges();
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