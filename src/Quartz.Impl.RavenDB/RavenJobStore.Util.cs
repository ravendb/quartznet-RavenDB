using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Quartz.Simpl;
using Quartz.Spi;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace Quartz.Impl.RavenDB
{
    public partial class RavenJobStore
    {
        public async Task SetSchedulerState(SchedulerState state, CancellationToken cancellationToken)
        {
            using var session = Store.OpenAsyncSession();
            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
            scheduler.State = state;
            await session.SaveChangesAsync(cancellationToken);
        }

        /// <summary>
        ///     Will recover any failed or misfired jobs and clean up the data store as
        ///     appropriate.
        /// </summary>
        /// <exception cref="JobPersistenceException">Condition.</exception>
        protected virtual async Task RecoverSchedulerData(IAsyncDocumentSession session,
            CancellationToken cancellationToken)
        {
            //
            // TODO: can further be optimized
            // 

            try
            {
                // update inconsistent states
                var queryResult = await session
                    .Query<Trigger>()
                    .Include(t => t.Scheduler) // pre-load scheduler
                    .Where(t =>
                        t.Scheduler == InstanceName && (t.State == InternalTriggerState.Acquired ||
                                                        t.State == InternalTriggerState.Blocked)
                    )
                    .ToListAsync(cancellationToken);
                foreach (var trigger in queryResult)
                {
                    var triggerToUpdate = await session.LoadAsync<Trigger>(trigger.Key, cancellationToken);
                    triggerToUpdate.State = InternalTriggerState.Waiting;
                }

                await session.SaveChangesAsync(cancellationToken);

                // recover jobs marked for recovery that were not fully executed
                IList<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                var queryResultJobs = await session
                    .Query<Job>()
                    .Where(j => j.Scheduler == InstanceName && j.RequestsRecovery)
                    .ToListAsync(cancellationToken);

                foreach (var job in queryResultJobs)
                    ((List<IOperableTrigger>) recoveringJobTriggers).AddRange(
                        await GetTriggersForJob(new JobKey(job.Name, job.Group), cancellationToken));

                foreach (var trigger in recoveringJobTriggers)
                    if (await CheckExists(trigger.JobKey, cancellationToken))
                    {
                        trigger.ComputeFirstFireTimeUtc(null);
                        await StoreTrigger(trigger, true, cancellationToken);
                    }

                // remove lingering 'complete' triggers...
                IList<Trigger> triggersInStateComplete = await session
                    .Query<Trigger>()
                    .Where(t => t.Scheduler == InstanceName && t.State == InternalTriggerState.Complete)
                    .ToListAsync(cancellationToken);

                foreach (var trigger in triggersInStateComplete)
                    await RemoveTrigger(new TriggerKey(trigger.Name, trigger.Group), cancellationToken);

                var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);
                scheduler.State = SchedulerState.Started;
                await session.SaveChangesAsync(cancellationToken);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
            }
        }

        /// <summary>
        ///     Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        protected virtual string GetFiredTriggerRecordId()
        {
            var value = Interlocked.Increment(ref _ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        public async Task<Dictionary<string, ICalendar>> RetrieveCalendarCollection(CancellationToken cancellationToken)
        {
            using var session = Store.OpenAsyncSession();
            var scheduler = await session.LoadAsync<Scheduler>(InstanceName, cancellationToken);

            if (scheduler is null)
                throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture,
                    "Scheduler with instance name '{0}' is null", InstanceName));

            if (scheduler.Calendars is null)
                throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture,
                    "Calendar collection in '{0}' is null", InstanceName));

            return scheduler.Calendars;
        }

        protected virtual async Task<bool> ApplyMisfire(Trigger trigger, CancellationToken cancellationToken)
        {
            var misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);

            var fireTimeUtc = trigger.NextFireTimeUtc;
            if (!fireTimeUtc.HasValue || fireTimeUtc.Value > misfireTime
                                      || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
                return false;

            ICalendar cal = null;
            if (trigger.CalendarName != null) cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);

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
            else if (fireTimeUtc.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        protected virtual async Task SetAllTriggersOfJobToState(JobKey jobKey, InternalTriggerState state,
            CancellationToken cancellationToken)
        {
            using var session = Store.OpenAsyncSession();
            var triggers = session.Query<Trigger>()
                .Where(t => Equals(t.Group, jobKey.Group) && Equals(t.JobName, jobKey.Name));

            foreach (var trig in triggers)
            {
                var triggerToUpdate = await session.LoadAsync<Trigger>(trig.Key, cancellationToken);
                triggerToUpdate.State = state;
            }

            await session.SaveChangesAsync(cancellationToken);
        }
    }
}