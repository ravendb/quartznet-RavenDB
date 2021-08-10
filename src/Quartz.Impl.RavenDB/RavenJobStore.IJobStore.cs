using Quartz.Impl.Matchers;
using Quartz.Impl.RavenDB.Util;
using Quartz.Simpl;
using Quartz.Spi;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB
{
    public partial class RavenJobStore
    {
        public async Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler, CancellationToken cancellationToken = default)
        {
            this.signaler = signaler;

            await new TriggerIndex().ExecuteAsync(DocumentStoreHolder.Store, token: cancellationToken);
            await new JobIndex().ExecuteAsync(DocumentStoreHolder.Store, token: cancellationToken);
        }

        public async Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var exists = await session.Advanced.ExistsAsync(InstanceName, cancellationToken);

                if (!exists)
                {
                    var scheduler = new Scheduler() { InstanceName = InstanceName };
                    await session.StoreAsync(scheduler, InstanceName, cancellationToken);
                    await session.SaveChangesAsync(cancellationToken);
                    return;
                }

                // Scheduler with same instance name already exists, recover persistent data
                try
                {
                    await RecoverSchedulerData(cancellationToken);
                }
                catch (SchedulerException se)
                {
                    throw new SchedulerConfigException("Failure occurred during job recovery.", se);
                }
            }
        }

        public async Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            await SetSchedulerState(SchedulerState.Paused, cancellationToken);
        }

        public async Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            await SetSchedulerState(SchedulerState.Resumed, cancellationToken);
        }

        public async Task Shutdown(CancellationToken cancellationToken = default)
        {
            await SetSchedulerState(SchedulerState.Shutdown, cancellationToken);
        }

        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            await StoreJob(newJob, true);
            await StoreTrigger(newTrigger, true);
        }

        public async Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                return sched.PausedJobGroups.Contains(groupName);
            }
        }

        public async Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            return (await GetPausedTriggerGroups(cancellationToken)).Contains(groupName);
        }

        public async Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                if (await session.Advanced.ExistsAsync(newJob.Key.GetDatabaseId(), cancellationToken))
                {
                    if (!replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException(newJob);
                    }
                }

                var job = new Job(newJob, InstanceName);

                // Store() overwrites if job id already exists
                await session.StoreAsync(job, job.Key, cancellationToken);
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace, CancellationToken cancellationToken = default)
        {
            using (var bulkInsert = DocumentStoreHolder.Store.BulkInsert(token: cancellationToken))
            {
                foreach (var pair in triggersAndJobs)
                {
                    // First store the current job
                    bulkInsert.Store(new Job(pair.Key, InstanceName), pair.Key.Key.Name + "/" + pair.Key.Key.Group);

                    // Storing all triggers for the current job
                    foreach (var trig in pair.Value)
                    {
                        if (!(trig is IOperableTrigger operTrig))
                        {
                            continue;
                        }
                        var trigger = new Trigger(operTrig, InstanceName);

                        if ((await GetPausedTriggerGroups()).Contains(operTrig.Key.Group) || (await GetPausedJobGroups(cancellationToken)).Contains(operTrig.JobKey.Group))
                        {
                            trigger.State = InternalTriggerState.Paused;
                            if ((await GetBlockedJobs(cancellationToken)).Contains(operTrig.GetJobDatabaseId()))
                            {
                                trigger.State = InternalTriggerState.PausedAndBlocked;
                            }
                        }
                        else if ((await GetBlockedJobs(cancellationToken)).Contains(operTrig.GetJobDatabaseId()))
                        {
                            trigger.State = InternalTriggerState.Blocked;
                        }

                        bulkInsert.Store(trigger, trigger.Key);
                    }
                }
                // bulkInsert is disposed - same effect as session.SaveChanges()
            }
        }

        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                if (!await session.Advanced.ExistsAsync(jobKey.GetDatabaseId(), cancellationToken))
                {
                    return false;
                }

                session.Delete(jobKey.GetDatabaseId());
                await session.SaveChangesAsync(cancellationToken);
            }

            return true;
        }

        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                foreach (var key in jobKeys)
                {
                    session.Delete(key);
                }

                await session.SaveChangesAsync(cancellationToken);

                return true;
            }
        }

        public async Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var job = await session.LoadAsync<Job>(jobKey.GetDatabaseId(), cancellationToken);

                return job?.Deserialize();
            }
        }

        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                if (await session.Advanced.ExistsAsync(newTrigger.Key.GetDatabaseId(), cancellationToken))
                {
                    if (!replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException(newTrigger);
                    }
                }

                if (!await session.Advanced.ExistsAsync(newTrigger.JobKey.GetDatabaseId(), cancellationToken))
                {
                    throw new JobPersistenceException("The job (" + newTrigger.JobKey + ") referenced by the trigger does not exist.");
                }

                var trigger = new Trigger(newTrigger, InstanceName);

                var isTriggerGroupPaused = await session
                    .Query<Trigger>()
                    .Include(t => t.Scheduler)
                        .Where(t => Equals(t.Group, newTrigger.Key.Group) && (t.State == InternalTriggerState.Paused || t.State == InternalTriggerState.PausedAndBlocked))
                        .AnyAsync(cancellationToken);

                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                var isJobGroupPaused = scheduler.PausedJobGroups.Contains(newTrigger.JobKey.Group);

                // make sure trigger group is not paused and that job is not blocked
                if (isTriggerGroupPaused || isJobGroupPaused)
                {
                    trigger.State = InternalTriggerState.Paused;
                    if (scheduler.BlockedJobs.Contains(newTrigger.GetJobDatabaseId()))
                    {
                        trigger.State = InternalTriggerState.PausedAndBlocked;
                    }
                }
                else if (scheduler.BlockedJobs.Contains(newTrigger.GetJobDatabaseId()))
                {
                    trigger.State = InternalTriggerState.Blocked;
                }

                // Overwrite if exists
                await session.StoreAsync(trigger, trigger.Key, cancellationToken);
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                if (!await session.Advanced.ExistsAsync(triggerKey.GetDatabaseId(), cancellationToken))
                {
                    return false;
                }

                // Request trigger and associated job
                var trigger = await session
                    .Include<Trigger>(t => t.JobKey)
                    .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);
                var job = (await session.LoadAsync<Job>(trigger.JobKey, cancellationToken)).Deserialize();

                // Delete trigger
                session.Delete(triggerKey.GetDatabaseId());

                // Check for more triggers            
                var hasMoreTriggers = await session
                        .Query<Trigger>()
                        .Where(t => Equals(t.JobName, job.Key.Name) && Equals(t.Group, job.Key.Group) && !Equals(t.Key, trigger.Key))
                        .AnyAsync(cancellationToken);

                // Remove the trigger's job if it is not associated with any other triggers
                if (!hasMoreTriggers && !job.Durable)
                {
                    session.Delete(job.Key.GetDatabaseId());
                    await signaler.NotifySchedulerListenersJobDeleted(job.Key, cancellationToken);
                }

                await session.SaveChangesAsync(cancellationToken);
            }

            return true;
        }

        public async Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken cancellationToken = default)
        {
            // Returns false in case at least one trigger removal fails
            var result = true;
            foreach (var key in triggerKeys)
            {
                result &= await RemoveTrigger(key, cancellationToken);
            }
            return result;
        }

        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger, CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken))
            {
                return false;
            }

            var wasRemoved = await RemoveTrigger(triggerKey, cancellationToken);

            if (wasRemoved)
            {
                await StoreTrigger(newTrigger, true, cancellationToken);
            }

            return wasRemoved;
        }

        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

                return trigger?.Deserialize();
            }
        }

        public async Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            bool answer;
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
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

        public async Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return await session.Advanced.ExistsAsync(jobKey.GetDatabaseId(), cancellationToken);
            }
        }

        public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return await session.Advanced.ExistsAsync(triggerKey.GetDatabaseId(), cancellationToken);
            }
        }

        public Task ClearAllSchedulingData(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers, CancellationToken cancellationToken = default)
        {
            var calendarCopy = calendar.Clone();

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                if (sched?.Calendars is null)
                {
                    throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture, "Scheduler with instance name '{0}' is null", InstanceName));
                }

                if (await CalendarExists(name, cancellationToken) && !replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture, "Calendar with name '{0}' already exists.", name));
                }

                // add or replace calendar
                sched.Calendars[name] = calendarCopy;

                if (!updateTriggers)
                {
                    return;
                }

                var triggersKeysToUpdate = await session
                    .Query<Trigger>()
                    .Where(t => t.CalendarName == name)
                    .Select(t => t.Key)
                    .ToListAsync(cancellationToken);

                if (triggersKeysToUpdate.Count == 0)
                {
                    await session.SaveChangesAsync(cancellationToken);
                    return;
                }

                foreach (var triggerKey in triggersKeysToUpdate)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(triggerKey, cancellationToken);
                    var trigger = triggerToUpdate.Deserialize();
                    trigger.UpdateWithNewCalendar(calendarCopy, misfireThreshold);
                    triggerToUpdate.UpdateFireTimes(trigger);
                }

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            if (await RetrieveCalendar(calName, cancellationToken) is null)
            {
                return false;
            }

            var calCollection = await RetrieveCalendarCollection(cancellationToken);

            calCollection.Remove(calName);

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                sched.Calendars = calCollection;
                await session.SaveChangesAsync(cancellationToken);
            }

            return true;
        }

        public async Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            var callCollection = await RetrieveCalendarCollection(cancellationToken);

            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return await session.Query<Job>().CountAsync(cancellationToken);
            }
        }

        public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return await session.Query<Trigger>().CountAsync(cancellationToken);
            }
        }

        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken)).Count;
        }

        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<JobKey>();

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var allJobs = await session.Query<Job>().ToListAsync(cancellationToken);

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

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            StringOperator op = matcher.CompareWithOperator;
            string compareToValue = matcher.CompareToValue;

            var result = new HashSet<TriggerKey>();

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var allTriggers = await session.Query<Trigger>().ToListAsync(cancellationToken);

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

        public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                return await session.Query<Job>()
                      .Select(j => j.Group)
                      .Distinct()
                      .ToListAsync(cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                try
                {
                    var result = await session.Query<Trigger>()
                        .Select(t => t.Group)
                        .Distinct()
                        .ToListAsync(cancellationToken);
                    return result;
                }
                catch (ArgumentNullException)
                {
                    return new List<string>();
                }
            }
        }

        public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken)).Keys.ToList();
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                try
                {
                    var result = (await session
                        .Query<Trigger>()
                        .Where(t => Equals(t.JobName, jobKey.Name) && Equals(t.Group, jobKey.Group))
                        .ToListAsync(cancellationToken))
                        .Select(trigger => trigger.Deserialize()).ToList();
                    return result;
                }
                catch (NullReferenceException)
                {
                    return new List<IOperableTrigger>();
                }
            }
        }

        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            Trigger trigger;

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                trigger = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);
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

        public async Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var trig = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

                // if the trigger doesn't exist or is "complete" pausing it does not make sense...
                if (trig is null)
                {
                    return;
                }

                if (trig.State == InternalTriggerState.Complete)
                {
                    return;
                }

                trig.State = trig.State == InternalTriggerState.Blocked ? InternalTriggerState.PausedAndBlocked : InternalTriggerState.Paused;

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            var pausedGroups = new HashSet<string>();

            var triggerKeysForMatchedGroup = await GetTriggerKeys(matcher, cancellationToken);

            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                await PauseTrigger(triggerKey, cancellationToken);
                pausedGroups.Add(triggerKey.Group);
            }

            return new HashSet<string>(pausedGroups);
        }

        public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken);

            foreach (var trigger in triggersForJob)
            {
                await PauseTrigger(trigger.Key, cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            var pausedGroups = new List<string>();

            var jobKeysForMatchedGroup = await GetJobKeys(matcher, cancellationToken);

            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                await PauseJob(jobKey, cancellationToken);
                pausedGroups.Add(jobKey.Group);

                using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                {
                    var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                    sched.PausedJobGroups.Add(matcher.CompareToValue);

                    await session.SaveChangesAsync(cancellationToken);
                }
            }

            return pausedGroups;
        }

        public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var trigger = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

                if (trigger is null)
                {
                    return;
                }

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused &&
                    trigger.State != InternalTriggerState.PausedAndBlocked)
                {
                    return;
                }

                trigger.State = (await GetBlockedJobs(cancellationToken)).Contains(trigger.JobKey) ? InternalTriggerState.Blocked : InternalTriggerState.Waiting;

                await ApplyMisfire(trigger, cancellationToken);

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();
            var keys = await GetTriggerKeys(matcher, cancellationToken);

            foreach (TriggerKey triggerKey in keys)
            {
                await ResumeTrigger(triggerKey, cancellationToken);
                resumedGroups.Add(triggerKey.Group);
            }

            return new List<string>(resumedGroups);
        }

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
                return await session
                    .Query<Trigger>()
                        .Where(t => t.State == InternalTriggerState.Paused || t.State == InternalTriggerState.PausedAndBlocked)
                        .Distinct()
                        .Select(t => t.Group)
                        .ToListAsync(cancellationToken);
        }

        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken);

            foreach (var trigger in triggersForJob)
            {
                await ResumeTrigger(trigger.Key, cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();

            var keys = await GetJobKeys(matcher, cancellationToken);

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

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
                await session.SaveChangesAsync(cancellationToken);
            }

            foreach (JobKey key in keys)
            {
                var triggers = await GetTriggersForJob(key, cancellationToken);
                foreach (IOperableTrigger trigger in triggers)
                {
                    await ResumeTrigger(trigger.Key, cancellationToken);
                }
            }

            return resumedGroups;
        }

        public async Task PauseAll(CancellationToken cancellationToken = default)
        {
            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

            foreach (var groupName in triggerGroupNames)
            {
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
            }
        }

        public async Task ResumeAll(CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                sched.PausedJobGroups.Clear();

                var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

                foreach (var groupName in triggerGroupNames)
                {
                    await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
                }
            }
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow, CancellationToken cancellationToken = default)
        {
            var result = new List<IOperableTrigger>();
            var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            DateTimeOffset? firstAcquiredTriggerFireTime = null;

            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var triggersQuery = await session
                    .Query<Trigger>()
                    .Where(t => (t.State == InternalTriggerState.Waiting) && (t.NextFireTimeUtc <= (noLaterThan + timeWindow).UtcDateTime))
                    .OrderBy(t => t.NextFireTimeTicks)
                    .ThenByDescending(t => t.Priority)
                    .ToListAsync(cancellationToken);

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

                    if (await ApplyMisfire(candidateTrigger, cancellationToken))
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
                    Job job = await session.LoadAsync<Job>(candidateTrigger.JobKey, cancellationToken);

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

                    if (firstAcquiredTriggerFireTime is null)
                    {
                        firstAcquiredTriggerFireTime = candidateTrigger.NextFireTimeUtc;
                    }

                    if (result.Count == maxCount)
                    {
                        break;
                    }
                }

                await session.SaveChangesAsync(cancellationToken);
            }
            return result;
        }

        public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var trig = await session.LoadAsync<Trigger>(trigger.GetDatabaseId(), cancellationToken);
                if ((trig is null) || (trig.State != InternalTriggerState.Acquired))
                {
                    return;
                }
                trig.State = InternalTriggerState.Waiting;
                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = default)
        {
            var results = new List<TriggerFiredResult>();
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                try
                {
                    foreach (IOperableTrigger tr in triggers)
                    {
                        // was the trigger deleted since being acquired?
                        var trigger = await session
                            .Include<Trigger>(t => t.JobKey)
                            .LoadAsync<Trigger>(tr.GetDatabaseId(), cancellationToken);

                        // was the trigger completed, paused, blocked, etc. since being acquired?
                        if (trigger?.State != InternalTriggerState.Acquired)
                        {
                            continue;
                        }

                        ICalendar cal = null;
                        if (trigger.CalendarName != null)
                        {
                            cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);
                            if (cal == null)
                            {
                                continue;
                            }
                        }
                        DateTimeOffset? prevFireTime = trigger.PreviousFireTimeUtc;

                        var trig = trigger.Deserialize();
                        trig.Triggered(cal);

                        var dbJob = (await session.LoadAsync<Job>(trig.JobKey.GetDatabaseId(), cancellationToken)).Deserialize();

                        TriggerFiredBundle bndle = new TriggerFiredBundle(
                            dbJob,
                            trig,
                            cal,
                            false, 
                            SystemTime.UtcNow(),
                            trig.GetPreviousFireTimeUtc(), 
                            prevFireTime,
                            trig.GetNextFireTimeUtc()
                            );

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
                            var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                            sched.BlockedJobs.Add(job.Key.Name + "/" + job.Key.Group);
                        }

                        results.Add(new TriggerFiredResult(bndle));
                    }
                }
                finally
                {
                    await session.SaveChangesAsync(cancellationToken);
                }
            }
            return results;
        }

        public async Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode, CancellationToken cancellationToken = default)
        {
            using (var session = DocumentStoreHolder.Store.OpenAsyncSession())
            {
                var entry = await session.LoadAsync<Trigger>(trigger.GetDatabaseId(), cancellationToken);
                var sched = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                // It's possible that the job or trigger is null if it was deleted during execution
                var job = await session.LoadAsync<Job>(trigger.GetJobDatabaseId(), cancellationToken);

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
                            var triggerToUpdate = await session.LoadAsync<Trigger>(t.Key, cancellationToken);
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
                        DateTimeOffset? d = trigger.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = entry.NextFireTimeUtc;
                            if (!d.HasValue)
                            {
                                await RemoveTrigger(trigger.Key, cancellationToken);
                            }
                            else
                            {
                                //Deleting cancelled - trigger still active
                            }
                        }
                        else
                        {
                            await RemoveTrigger(trigger.Key, cancellationToken);
                            signaler.SignalSchedulingChange(null);
                        }
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerComplete)
                    {
                        entry.State = InternalTriggerState.Complete;
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetTriggerError)
                    {
                        entry.State = InternalTriggerState.Error;
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersError)
                    {
                        await SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Error, cancellationToken);
                        signaler.SignalSchedulingChange(null);
                    }
                    else if (triggerInstCode == SchedulerInstruction.SetAllJobTriggersComplete)
                    {
                        await SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Complete, cancellationToken);
                        signaler.SignalSchedulingChange(null);
                    }
                }
                await session.SaveChangesAsync(cancellationToken);
            }
        }
    }
}
