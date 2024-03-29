﻿using Quartz.Impl.Matchers;
using Quartz.Impl.RavenDB.Util;
using Quartz.Simpl;
using Quartz.Spi;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;

namespace Quartz.Impl.RavenDB
{
    public partial class RavenJobStore
    {
        public RavenJobStore()
        {

        }

        public RavenJobStore(IDocumentStore store)
        {
            Store = store;
        }

        public Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler,
            CancellationToken cancellationToken = default)
        {
            _signaler = signaler;

            Store = Store ?? InitializeDocumentStore();
            return Task.CompletedTask;
        }

        protected virtual IDocumentStore InitializeDocumentStore()
        {
            var s = new DocumentStore
            {
                Urls = _urls,
                Database = Database,
                Certificate = string.IsNullOrEmpty(CertPath) ? null : new X509Certificate2(CertPath, CertPass)
            };

            s.OnBeforeQuery += (sender, beforeQueryExecutedArgs) =>
            {
                beforeQueryExecutedArgs.QueryCustomization.WaitForNonStaleResults();
            };
            s.Initialize();

            return s;
        }

        public async Task SchedulerStarted(CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

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
                await RecoverSchedulerData(session, cancellationToken).ConfigureAwait(false);
            }
            catch (SchedulerException se)
            {
                throw new SchedulerConfigException("Failure occurred during job recovery.", se);
            }
        }

        public async Task SchedulerPaused(CancellationToken cancellationToken = default)
        {
            await SetSchedulerState(SchedulerState.Paused, cancellationToken).ConfigureAwait(false);
        }

        public async Task SchedulerResumed(CancellationToken cancellationToken = default)
        {
            await SetSchedulerState(SchedulerState.Resumed, cancellationToken).ConfigureAwait(false);
        }

        public async Task Shutdown(CancellationToken cancellationToken = default)
        {
            await SetSchedulerState(SchedulerState.Shutdown, cancellationToken).ConfigureAwait(false);
        }

        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            await StoreJob(newJob, true, cancellationToken).ConfigureAwait(false);
            await StoreTrigger(newTrigger, true, cancellationToken).ConfigureAwait(false);
        }

        public async Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            return await session
                .Query<Scheduler>()
                .Where(s =>
                    Equals(s.InstanceName, InstanceName) &&
                    s.PausedJobGroups.Contains(groupName))
                .AnyAsync(cancellationToken);
        }

        public async Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = default)
        {
            return (await GetPausedTriggerGroups(cancellationToken).ConfigureAwait(false)).Contains(groupName);
        }

        public async Task StoreJob(IJobDetail newJob, bool replaceExisting,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            if (await session.Advanced.ExistsAsync(newJob.Key.GetDatabaseId(), cancellationToken))
                if (!replaceExisting)
                    throw new ObjectAlreadyExistsException(newJob);

            var job = new Job(newJob, InstanceName);

            // Store() overwrites if job id already exists
            await session.StoreAsync(job, job.Key, cancellationToken);
            await session.SaveChangesAsync(cancellationToken);
        }

        public async Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            await using var bulkInsert = Store.BulkInsert(token: cancellationToken);

            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            foreach (var pair in triggersAndJobs)
            {
                // First store the current job
                await bulkInsert.StoreAsync(new Job(pair.Key, InstanceName), pair.Key.Key.GetDatabaseId());

                // Storing all triggers for the current job
                foreach (var orig in pair.Value.OfType<IOperableTrigger>())
                {
                    var trigger = new Trigger(orig, InstanceName);

                    var isInPausedTriggerGroup = await session
                        .Query<Trigger>()
                        .Where(t => (
                                        t.State == InternalTriggerState.Paused ||
                                        t.State == InternalTriggerState.PausedAndBlocked
                                    )
                                    && Equals(t.Group, orig.Key.Group))
                        .AnyAsync(cancellationToken);

                    if (isInPausedTriggerGroup || scheduler.PausedJobGroups.Contains(orig.JobKey.Group))
                    {
                        trigger.State = InternalTriggerState.Paused;

                        if (scheduler.BlockedJobs.Contains(orig.GetJobDatabaseId()))
                            trigger.State = InternalTriggerState.PausedAndBlocked;
                    }
                    else if (scheduler.BlockedJobs.Contains(orig.GetJobDatabaseId()))
                    {
                        trigger.State = InternalTriggerState.Blocked;
                    }

                    await bulkInsert.StoreAsync(trigger, trigger.Key);
                }
            }
            // bulkInsert is disposed - same effect as session.SaveChanges()
        }

        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            if (!await session.Advanced.ExistsAsync(jobKey.GetDatabaseId(), cancellationToken))
            {
                return false;
            }

            session.Delete(jobKey.GetDatabaseId());

            await session.SaveChangesAsync(cancellationToken);

            return true;
        }

        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            foreach (var key in jobKeys) 
                session.Delete(key);

            await session.SaveChangesAsync(cancellationToken);

            return true;
        }

        public async Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            var job = await session.LoadAsync<Job>(jobKey.GetDatabaseId(), cancellationToken);

            return job?.Deserialize();
        }

        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            if (await session.Advanced.ExistsAsync(newTrigger.Key.GetDatabaseId(), cancellationToken))
                if (!replaceExisting)
                    throw new ObjectAlreadyExistsException(newTrigger);

            if (!await session.Advanced.ExistsAsync(newTrigger.JobKey.GetDatabaseId(), cancellationToken))
                throw new JobPersistenceException("The job (" + newTrigger.JobKey +
                                                  ") referenced by the trigger does not exist.");

            var trigger = new Trigger(newTrigger, InstanceName);

            var isTriggerGroupPaused = await session
                .Query<Trigger>()
                .Include(t => t.Scheduler)
                .Where(t =>
                    Equals(t.Group, newTrigger.Key.Group)
                    && (t.State == InternalTriggerState.Paused ||
                        t.State == InternalTriggerState.PausedAndBlocked))
                .AnyAsync(cancellationToken);

            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            if (scheduler != null)
            {
                var isJobGroupPaused = scheduler.PausedJobGroups.Contains(newTrigger.JobKey.Group);

                // make sure trigger group is not paused and that job is not blocked
                if (isTriggerGroupPaused || isJobGroupPaused)
                {
                    trigger.State = InternalTriggerState.Paused;

                    if (scheduler.BlockedJobs.Contains(newTrigger.GetJobDatabaseId()))
                        trigger.State = InternalTriggerState.PausedAndBlocked;
                }
                else if (scheduler.BlockedJobs.Contains(newTrigger.GetJobDatabaseId()))
                {
                    trigger.State = InternalTriggerState.Blocked;
                }
            }

            // Overwrite if exists
            await session.StoreAsync(trigger, trigger.Key, cancellationToken);
            await session.SaveChangesAsync(cancellationToken);
        }

        private async Task RemoveTriggerOnly(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            session.Delete(triggerKey.GetDatabaseId());
            await session.SaveChangesAsync(cancellationToken);
        }

        public async Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            if (!await session.Advanced.ExistsAsync(triggerKey.GetDatabaseId(), cancellationToken))
            {
                return false;
            }

            // Request trigger and associated job
            var trigger = await session
                .Include<Trigger>(t => t.JobKey)
                .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);
            // Get pre-loaded associated job
            var job = (await session.LoadAsync<Job>(trigger.JobKey, cancellationToken)).Deserialize();
            
            // Check for more triggers
            var hasMoreTriggers = await session
                .Query<Trigger>()
                .Where(t =>
                    Equals(t.JobName, job.Key.Name) &&
                    Equals(t.Group, job.Key.Group) &&
                    !Equals(t.Key, trigger.Key)) // exclude our own since not yet deleted
                .AnyAsync(cancellationToken);

            // Remove the trigger's job if it is not associated with any other triggers
            if (!hasMoreTriggers && !job.Durable)
            {
                session.Delete(job.Key.GetDatabaseId());
                await _signaler.NotifySchedulerListenersJobDeleted(job.Key, cancellationToken);
            }

            // Delete trigger
            session.Delete(triggerKey.GetDatabaseId());

            await session.SaveChangesAsync(cancellationToken);

            return true;
        }

        public async Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys,
            CancellationToken cancellationToken = default)
        {
            // Returns false in case at least one trigger removal fails
            var result = true;
            //
            // TODO: convert to one bulk DB session
            // 
            foreach (var key in triggerKeys) result &= await RemoveTrigger(key, cancellationToken);
            return result;
        }

        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = default)
        {
            if (!await CheckExists(triggerKey, cancellationToken).ConfigureAwait(false)) 
                return false;

            await RemoveTriggerOnly(triggerKey, cancellationToken).ConfigureAwait(false);

            await StoreTrigger(newTrigger, true, cancellationToken).ConfigureAwait(false);

            return true;
        }

        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            var trigger = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

            return trigger?.Deserialize();
        }

        public async Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            return scheduler?.Calendars is { } && scheduler.Calendars.ContainsKey(calName);
        }

        public async Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            return await session.Advanced.ExistsAsync(jobKey.GetDatabaseId(), cancellationToken);
        }

        public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            return await session.Advanced.ExistsAsync(triggerKey.GetDatabaseId(), cancellationToken);
        }

        public async Task ClearAllSchedulingData(CancellationToken cancellationToken)
        {
            await (await Store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery
            {
                QueryParameters = new Parameters()
                {
                    {"name", InstanceName}
                },
                Query = "from Jobs j where j.Scheduler == $name"
            }), token: cancellationToken)).WaitForCompletionAsync(TimeSpan.FromSeconds(10));

            await (await Store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery
            {
                QueryParameters = new Parameters()
                {
                    {"name", InstanceName}
                },
                Query = "from Triggers t where t.Scheduler == $name"
            }), token: cancellationToken)).WaitForCompletionAsync(TimeSpan.FromSeconds(10));
        }

        public async Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers,
            CancellationToken cancellationToken = default)
        {
            var calendarCopy = calendar.Clone();

            using var session = Store.OpenAsyncSession();

            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            if (scheduler?.Calendars is null)
                throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture,
                    "Scheduler with instance name '{0}' is null", InstanceName));

            if (await CalendarExists(name, cancellationToken).ConfigureAwait(false) && !replaceExisting)
                throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture,
                    "Calendar with name '{0}' already exists.", name));

            // add or replace calendar
            scheduler.Calendars[name] = calendarCopy;

            if (!updateTriggers) return;

            var triggersKeysToUpdate = await session
                .Query<Trigger>()
                .Where(t => Equals(t.CalendarName, name))
                .Select(t => t.Key)
                .ToListAsync(cancellationToken);

            if (triggersKeysToUpdate.Any())
            {
                await session.SaveChangesAsync(cancellationToken);
                return;
            }

            foreach (var triggerKey in triggersKeysToUpdate)
            {
                var triggerToUpdate = await session.LoadAsync<Trigger>(triggerKey, cancellationToken);
                var trigger = triggerToUpdate.Deserialize();
                trigger.UpdateWithNewCalendar(calendarCopy, _misfireThreshold);
                triggerToUpdate.UpdateFireTimes(trigger);
            }

            await session.SaveChangesAsync(cancellationToken);
        }

        public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            if (await RetrieveCalendar(calName, cancellationToken).ConfigureAwait(false) is null) return false;

            var calCollection = await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false);

            calCollection.Remove(calName);

            using var session = Store.OpenAsyncSession();

            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
            scheduler.Calendars = calCollection;
            await session.SaveChangesAsync(cancellationToken);

            return true;
        }

        public async Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = default)
        {
            var callCollection = await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false);

            return callCollection.ContainsKey(calName) ? callCollection[calName] : null;
        }

        public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            return await session.Query<Job>().CountAsync(cancellationToken);
        }

        public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            return await session.Query<Trigger>().CountAsync(cancellationToken);
        }

        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false)).Count;
        }

        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var op = matcher.CompareWithOperator;
            var compareToValue = matcher.CompareToValue;

            var result = new HashSet<JobKey>();

            using var session = Store.OpenAsyncSession();

            var allJobs = await session.Query<Job>().ToListAsync(cancellationToken);

            foreach (var job in allJobs.Where(job => op.Evaluate(job.Group, compareToValue)))
                result.Add(new JobKey(job.Name, job.Group));

            return result;
        }

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var op = matcher.CompareWithOperator;
            var compareToValue = matcher.CompareToValue;

            var result = new HashSet<TriggerKey>();

            using var session = Store.OpenAsyncSession();

            var allTriggers = await session.Query<Trigger>().ToListAsync(cancellationToken);

            foreach (var trigger in allTriggers.Where(trigger => op.Evaluate(trigger.Group, compareToValue)))
                result.Add(new TriggerKey(trigger.Name, trigger.Group));

            return result;
        }

        public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            return (await session.Query<Job>()
                .GroupBy(j => j.Group)
                .Select(x=>new {G = x.Key})
                .ToListAsync(cancellationToken))
                .Select(x=>x.G).ToList();
        }

        public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            return (await session.Query<Trigger>()
                    .GroupBy(j => j.Group)
                    .Select(x=>new {G = x.Key})
                    .ToListAsync(cancellationToken))
                .Select(x=>x.G).ToList();
        }

        public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = default)
        {
            return (await RetrieveCalendarCollection(cancellationToken).ConfigureAwait(false)).Keys.ToList();
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            return (await session
                    .Query<Trigger>()
                    .Where(t => Equals(t.JobName, jobKey.Name) && Equals(t.Group, jobKey.Group))
                    .ToListAsync(cancellationToken))
                .Select(trigger => trigger.Deserialize()).ToList();
        }

        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            var trigger = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

            if (trigger is null) return TriggerState.None;

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
            using var session = Store.OpenAsyncSession();

            var trig = await session.LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

            // if the trigger doesn't exist or is "complete" pausing it does not make sense...
            if (trig is null) return;

            if (trig.State == InternalTriggerState.Complete) return;

            trig.State = trig.State == InternalTriggerState.Blocked
                ? InternalTriggerState.PausedAndBlocked
                : InternalTriggerState.Paused;

            await session.SaveChangesAsync(cancellationToken);
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var pausedGroups = new HashSet<string>();

            var triggerKeysForMatchedGroup = await GetTriggerKeys(matcher, cancellationToken).ConfigureAwait(false);

            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                await PauseTrigger(triggerKey, cancellationToken).ConfigureAwait(false);
                pausedGroups.Add(triggerKey.Group);
            }

            return new HashSet<string>(pausedGroups);
        }

        public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken).ConfigureAwait(false);

            foreach (var trigger in triggersForJob)
                await PauseTrigger(trigger.Key, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var pausedGroups = new List<string>();

            var jobKeysForMatchedGroup = await GetJobKeys(matcher, cancellationToken);

            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                await PauseJob(jobKey, cancellationToken);
                pausedGroups.Add(jobKey.Group);

                //
                // TODO: optimize
                // 
                using (var session = Store.OpenAsyncSession())
                {
                    var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                    scheduler.PausedJobGroups.Add(matcher.CompareToValue);

                    await session.SaveChangesAsync(cancellationToken);
                }
            }

            return pausedGroups;
        }

        public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = default)
        {
            using (var session = Store.OpenAsyncSession())
            {
                var trigger = await session
                    .Include<Trigger>(t => t.Scheduler) // preload
                    .LoadAsync<Trigger>(triggerKey.GetDatabaseId(), cancellationToken);

                if (trigger is null) return;

                var blocked = (await session.LoadAsync<Scheduler>(InstanceName, cancellationToken)).BlockedJobs;

                // if the trigger is not paused resuming it does not make sense...
                if (trigger.State != InternalTriggerState.Paused &&
                    trigger.State != InternalTriggerState.PausedAndBlocked)
                    return;

                trigger.State = blocked.Contains(trigger.JobKey)
                    ? InternalTriggerState.Blocked
                    : InternalTriggerState.Waiting;

                await ApplyMisfire(trigger, cancellationToken);

                await session.SaveChangesAsync(cancellationToken);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();
            var keys = await GetTriggerKeys(matcher, cancellationToken);

            foreach (var triggerKey in keys)
            {
                await ResumeTrigger(triggerKey, cancellationToken);
                resumedGroups.Add(triggerKey.Group);
            }

            return new List<string>(resumedGroups);
        }

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();
            return await session
                .Query<Trigger>()
                .Where(t =>
                    t.State == InternalTriggerState.Paused ||
                    t.State == InternalTriggerState.PausedAndBlocked)
                .Distinct()
                .Select(t => t.Group)
                .ToListAsync(cancellationToken);
        }

        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken);

            foreach (var trigger in triggersForJob) await ResumeTrigger(trigger.Key, cancellationToken);
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = default)
        {
            var resumedGroups = new HashSet<string>();

            var keys = await GetJobKeys(matcher, cancellationToken);

            using (var session = Store.OpenAsyncSession())
            {
                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                foreach (var pausedJobGroup in scheduler.PausedJobGroups.Where(pausedJobGroup =>
                    matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue)))
                    resumedGroups.Add(pausedJobGroup);

                foreach (var resumedGroup in resumedGroups) scheduler.PausedJobGroups.Remove(resumedGroup);

                await session.SaveChangesAsync(cancellationToken);
            }

            foreach (var key in keys)
            {
                var triggers = await GetTriggersForJob(key, cancellationToken);

                foreach (var trigger in triggers) await ResumeTrigger(trigger.Key, cancellationToken);
            }

            return resumedGroups;
        }

        public async Task PauseAll(CancellationToken cancellationToken = default)
        {
            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

            foreach (var groupName in triggerGroupNames)
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
        }

        public async Task ResumeAll(CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            scheduler.PausedJobGroups.Clear();

            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

            foreach (var groupName in triggerGroupNames)
                await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan,
            int maxCount, TimeSpan timeWindow, CancellationToken cancellationToken = default)
        {
            var result = new List<IOperableTrigger>();
            var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

            using var session = Store.OpenAsyncSession();

            var triggersQuery = await session
                .Query<Trigger>()
                .Where(t =>
                    t.State == InternalTriggerState.Waiting &&
                    t.NextFireTimeUtc <= (noLaterThan + timeWindow).UtcDateTime)
                .OrderBy(t => t.NextFireTimeTicks)
                .ThenByDescending(t => t.Priority)
                .ToListAsync(cancellationToken);

            var triggers = new SortedSet<Trigger>(triggersQuery, new TriggerComparator());

            while (true)
            {
                // return empty list if store has no such triggers.
                if (!triggers.Any()) return result;

                var candidateTrigger = triggers.First();
                if (candidateTrigger == null) break;
                if (!triggers.Remove(candidateTrigger)) break;
                if (candidateTrigger.NextFireTimeUtc == null) continue;

                if (await ApplyMisfire(candidateTrigger, cancellationToken))
                {
                    if (candidateTrigger.NextFireTimeUtc != null) triggers.Add(candidateTrigger);
                    continue;
                }

                if (candidateTrigger.NextFireTimeUtc > noLaterThan + timeWindow) break;

                // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                // put it back into the timeTriggers set and continue to search for next trigger.
                var jobKey = new JobKey(candidateTrigger.JobName, candidateTrigger.Group);
                var job = await session.LoadAsync<Job>(candidateTrigger.JobKey, cancellationToken);

                if (job.ConcurrentExecutionDisallowed)
                {
                    if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        continue; // go to next trigger in store.
                    acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                }

                candidateTrigger.State = InternalTriggerState.Acquired;
                candidateTrigger.FireInstanceId = GetFiredTriggerRecordId();

                result.Add(candidateTrigger.Deserialize());


                if (result.Count == maxCount) break;
            }

            await session.SaveChangesAsync(cancellationToken);

            return result;
        }

        public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger,
            CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            var trig = await session.LoadAsync<Trigger>(trigger.GetDatabaseId(), cancellationToken);
            if (trig is null || trig.State != InternalTriggerState.Acquired) return;
            trig.State = InternalTriggerState.Waiting;

            await session.SaveChangesAsync(cancellationToken);
        }

        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = default)
        {
            var results = new List<TriggerFiredResult>();

            using var session = Store.OpenAsyncSession();

            try
            {
                foreach (var tr in triggers)
                {
                    // was the trigger deleted since being acquired?
                    var trigger = await session
                        .Include<Trigger>(t => t.JobKey) // pre-load
                        .LoadAsync<Trigger>(tr.GetDatabaseId(), cancellationToken);

                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (trigger?.State != InternalTriggerState.Acquired) continue;

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);
                        if (cal == null) continue;
                    }

                    var prevFireTime = trigger.PreviousFireTimeUtc;

                    var trig = trigger.Deserialize();
                    trig.Triggered(cal);

                    // cached load
                    var dbJob = (await session.LoadAsync<Job>(trig.JobKey.GetDatabaseId(), cancellationToken))
                        .Deserialize();

                    var bundle = new TriggerFiredBundle(
                        dbJob,
                        trig,
                        cal,
                        false,
                        SystemTime.UtcNow(),
                        trig.GetPreviousFireTimeUtc(),
                        prevFireTime,
                        trig.GetNextFireTimeUtc()
                    );

                    var job = bundle.JobDetail;

                    trigger.UpdateFireTimes(trig);
                    trigger.State = InternalTriggerState.Waiting;

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        var triggerCollection = await session
                            .Query<Trigger>()
                            .Include(t => t.Scheduler) // pre-load
                            .Where(t =>
                                Equals(t.Group, job.Key.Group) &&
                                Equals(t.JobName, job.Key.Name))
                            .ToListAsync(cancellationToken);

                        foreach (var t in triggerCollection)
                        {
                            if (t.State == InternalTriggerState.Waiting)
                                t.State = InternalTriggerState.Blocked;
                            if (t.State == InternalTriggerState.Paused)
                                t.State = InternalTriggerState.PausedAndBlocked;
                        }

                        var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

                        scheduler.BlockedJobs.Add(job.Key.GetDatabaseId());
                    }

                    results.Add(new TriggerFiredResult(bundle));
                }
            }
            finally
            {
                await session.SaveChangesAsync(cancellationToken);
            }

            return results;
        }

        public async Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode, CancellationToken cancellationToken = default)
        {
            using var session = Store.OpenAsyncSession();

            var entry = await session
                .Include<Trigger>(t => t.Scheduler) // pre-load scheduler
                .Include<Trigger>(t => t.JobKey) // pre-load job
                .LoadAsync<Trigger>(trigger.GetDatabaseId(), cancellationToken);

            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            // It's possible that the job or trigger is null if it was deleted during execution
            var job = await session.LoadAsync<Job>(trigger.GetJobDatabaseId(), cancellationToken);

            if (job != null)
            {
                if (jobDetail.PersistJobDataAfterExecution) job.JobDataMap = jobDetail.JobDataMap;

                if (job.ConcurrentExecutionDisallowed)
                {
                    scheduler.BlockedJobs.Remove(job.Key);

                    var triggerCollection = await session.Query<Trigger>()
                        .Where(t => Equals(t.Group, job.Group) && Equals(t.JobName, job.Name))
                        .ToListAsync(cancellationToken);

                    foreach (var t in triggerCollection)
                    {
                        var triggerToUpdate = await session.LoadAsync<Trigger>(t.Key, cancellationToken);

                        if (t.State == InternalTriggerState.Blocked)
                            triggerToUpdate.State = InternalTriggerState.Waiting;
                        if (t.State == InternalTriggerState.PausedAndBlocked)
                            triggerToUpdate.State = InternalTriggerState.Paused;
                    }

                    _signaler.SignalSchedulingChange(null, cancellationToken);
                }
            }
            else
            {
                // even if it was deleted, there may be cleanup to do
                scheduler.BlockedJobs.Remove(jobDetail.Key.GetDatabaseId());
            }

            // check for trigger deleted during execution...
            if (trigger != null)
            {
                switch (triggerInstCode)
                {
                    case SchedulerInstruction.DeleteTrigger:
                    {
                        // Deleting triggers
                        var d = trigger.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = entry.NextFireTimeUtc;
                            if (!d.HasValue)
                            {
                                await RemoveTrigger(trigger.Key, cancellationToken);
                            }
                        }
                        else
                        {
                            await RemoveTrigger(trigger.Key, cancellationToken);
                            _signaler.SignalSchedulingChange(null, cancellationToken);
                        }

                        break;
                    }
                    case SchedulerInstruction.SetTriggerComplete:
                        entry.State = InternalTriggerState.Complete;
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                    case SchedulerInstruction.SetTriggerError:
                        entry.State = InternalTriggerState.Error;
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersError:
                        await SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Error, cancellationToken);
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersComplete:
                        await SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Complete,
                            cancellationToken);
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                }
            }

            await session.SaveChangesAsync(cancellationToken);
        }
    }
}
