#region License

/* 
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 */

#endregion

using System;
using System.Collections.Generic;
using NUnit.Framework;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB.Tests
{
    /// <summary>
    /// Unit tests for RavenJobStore, based on the RAMJobStore tests with minor changes
    /// (originally submitted by Johannes Zillmann)
    /// </summary>
    [TestFixture]
    public class RavenJobStoreUnitTests
    {
        private IJobStore fJobStore;
        private JobDetailImpl fJobDetail;
        private SampleSignaler fSignaler;

        [SetUp]
        public void SetUp()
        {
            fJobStore = new RavenJobStore();
            fJobStore.ClearAllSchedulingData();
            Thread.Sleep(1000);
        }

        private void InitJobStore()
        {

            fJobStore = new RavenJobStore();

            fJobStore.ClearAllSchedulingData();
            fSignaler = new SampleSignaler();
            fJobStore.Initialize(null, fSignaler);
            fJobStore.SchedulerStarted();

            fJobDetail = new JobDetailImpl("job1", "jobGroup1", typeof(NoOpJob)) {Durable = true};
            fJobStore.StoreJob(fJobDetail, true);
        }

        [Test]
        public async Task TestAcquireNextTrigger()
        {
            InitJobStore();

            var d = DateBuilder.EvenMinuteDateAfterNow();
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, d.AddSeconds(200), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, d.AddSeconds(50), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger1", "triggerGroup2", fJobDetail.Name,
                fJobDetail.Group, d.AddSeconds(100), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));

            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            await fJobStore.StoreTrigger(trigger1, false);
            await fJobStore.StoreTrigger(trigger2, false);
            await fJobStore.StoreTrigger(trigger3, false);

            var firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            Assert.AreEqual(0, (await fJobStore.AcquireNextTriggers(d.AddMilliseconds(10), 1, TimeSpan.Zero)).Count);
            Assert.AreEqual(trigger2,
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)).ToArray()[0]);
            Assert.AreEqual(trigger3,
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)).ToArray()[0]);
            Assert.AreEqual(trigger1,
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)).ToArray()[0]);
            Assert.AreEqual(0,
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)).Count);


            // release trigger3
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            Assert.AreEqual(trigger3,
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1)))
                .ToArray()[0]);
        }

        [Test]
        public async Task TestAcquireNextTriggerBatch()
        {
            InitJobStore();

            var d = DateBuilder.EvenMinuteDateAfterNow();

            IOperableTrigger early = new SimpleTriggerImpl("early", "triggerGroup1", fJobDetail.Name, fJobDetail.Group,
                d, d.AddMilliseconds(5), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, d.AddMilliseconds(200000), d.AddMilliseconds(200005), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, d.AddMilliseconds(200100), d.AddMilliseconds(200105), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger3", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, d.AddMilliseconds(200200), d.AddMilliseconds(200205), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger4 = new SimpleTriggerImpl("trigger4", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, d.AddMilliseconds(200300), d.AddMilliseconds(200305), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger10 = new SimpleTriggerImpl("trigger10", "triggerGroup2", fJobDetail.Name,
                fJobDetail.Group, d.AddMilliseconds(500000), d.AddMilliseconds(700000), 2, TimeSpan.FromSeconds(2));

            early.ComputeFirstFireTimeUtc(null);
            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            trigger4.ComputeFirstFireTimeUtc(null);
            trigger10.ComputeFirstFireTimeUtc(null);
            await fJobStore.StoreTrigger(early, false);
            await fJobStore.StoreTrigger(trigger1, false);
            await fJobStore.StoreTrigger(trigger2, false);
            await fJobStore.StoreTrigger(trigger3, false);
            await fJobStore.StoreTrigger(trigger4, false);
            await fJobStore.StoreTrigger(trigger10, false);

            var firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            var acquiredTriggers =
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 4, TimeSpan.FromSeconds(1)))
                .ToList();
            Assert.AreEqual(4, acquiredTriggers.Count);
            Assert.AreEqual(early.Key, acquiredTriggers[0].Key);
            Assert.AreEqual(trigger1.Key, acquiredTriggers[1].Key);
            Assert.AreEqual(trigger2.Key, acquiredTriggers[2].Key);
            Assert.AreEqual(trigger3.Key, acquiredTriggers[3].Key);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);

            acquiredTriggers =
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 5, TimeSpan.FromMilliseconds(1000)))
                .ToList();
            Assert.AreEqual(5, acquiredTriggers.Count);
            Assert.AreEqual(early.Key, acquiredTriggers[0].Key);
            Assert.AreEqual(trigger1.Key, acquiredTriggers[1].Key);
            Assert.AreEqual(trigger2.Key, acquiredTriggers[2].Key);
            Assert.AreEqual(trigger3.Key, acquiredTriggers[3].Key);
            Assert.AreEqual(trigger4.Key, acquiredTriggers[4].Key);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            await fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers =
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 6, TimeSpan.FromSeconds(1)))
                .ToList();
            Assert.AreEqual(5, acquiredTriggers.Count);
            Assert.AreEqual(early.Key, acquiredTriggers[0].Key);
            Assert.AreEqual(trigger1.Key, acquiredTriggers[1].Key);
            Assert.AreEqual(trigger2.Key, acquiredTriggers[2].Key);
            Assert.AreEqual(trigger3.Key, acquiredTriggers[3].Key);
            Assert.AreEqual(trigger4.Key, acquiredTriggers[4].Key);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            await fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = (await fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(1), 5, TimeSpan.Zero))
                .ToList();
            Assert.AreEqual(2, acquiredTriggers.Count);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);

            acquiredTriggers =
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(250), 5,
                    TimeSpan.FromMilliseconds(199))).ToList();
            Assert.AreEqual(5, acquiredTriggers.Count);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
            await fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers =
                (await fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(150), 5,
                    TimeSpan.FromMilliseconds(50L))).ToList();
            Assert.AreEqual(4, acquiredTriggers.Count);
            await fJobStore.ReleaseAcquiredTrigger(early);
            await fJobStore.ReleaseAcquiredTrigger(trigger1);
            await fJobStore.ReleaseAcquiredTrigger(trigger2);
            await fJobStore.ReleaseAcquiredTrigger(trigger3);
        }

        [Test]
        public async Task TestTriggerStates()
        {
            InitJobStore();

            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2,
                TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            Assert.AreEqual(TriggerState.None, fJobStore.GetTriggerState(trigger.Key));
            await fJobStore.StoreTrigger(trigger, false);
            Assert.AreEqual(TriggerState.Normal, fJobStore.GetTriggerState(trigger.Key));

            await fJobStore.PauseTrigger(trigger.Key);
            Assert.AreEqual(TriggerState.Paused, fJobStore.GetTriggerState(trigger.Key));

            await fJobStore.ResumeTrigger(trigger.Key);
            Assert.AreEqual(TriggerState.Normal, fJobStore.GetTriggerState(trigger.Key));

            trigger = (await fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1,
                TimeSpan.FromMilliseconds(1))).ToArray()[0];
            Assert.IsNotNull(trigger);
            await fJobStore.ReleaseAcquiredTrigger(trigger);
            trigger = (await fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1,
                TimeSpan.FromMilliseconds(1))).ToArray()[0];
            Assert.IsNotNull(trigger);
            Assert.AreEqual(0,
                (await fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1,
                    TimeSpan.FromMilliseconds(1))).Count);
        }

        [Test]
        public void TestRemoveCalendarWhenTriggersPresent()
        {
            InitJobStore();

            // QRTZNET-29

            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name,
                fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2,
                TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            ICalendar cal = new MonthlyCalendar();
            fJobStore.StoreTrigger(trigger, false);
            fJobStore.StoreCalendar("cal", cal, false, true);

            fJobStore.RemoveCalendar("cal");
        }

        [Test]
        public async Task TestStoreTriggerReplacesTrigger()
        {
            InitJobStore();

            var jobName = "StoreJobReplacesJob";
            var jobGroup = "StoreJobReplacesJobGroup";
            var detail = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            await fJobStore.StoreJob(detail, false);

            var trName = "StoreTriggerReplacesTrigger";
            var trGroup = "StoreTriggerReplacesTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.Now);
            tr.JobKey = new JobKey(jobName, jobGroup);
            tr.CalendarName = null;

            await fJobStore.StoreTrigger(tr, false);
            Assert.AreEqual(tr, fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)));

            tr.CalendarName = "NonExistingCalendar";
            await fJobStore.StoreTrigger(tr, true);
            Assert.AreEqual(tr, fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)));
            Assert.AreEqual(tr.CalendarName,
                (await fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)))?.CalendarName,
                "StoreJob doesn't replace triggers");

            var exceptionRaised = false;
            try
            {
                await fJobStore.StoreTrigger(tr, false);
            }
            catch (ObjectAlreadyExistsException)
            {
                exceptionRaised = true;
            }

            Assert.IsTrue(exceptionRaised, "an attempt to store duplicate trigger succeeded");
        }

        [Test]
        public void PauseJobGroupPausesNewJob()
        {
            InitJobStore();

            var jobName1 = "PauseJobGroupPausesNewJob";
            var jobName2 = "PauseJobGroupPausesNewJob2";
            var jobGroup = "PauseJobGroupPausesNewJobGroup";
            var detail = new JobDetailImpl(jobName1, jobGroup, typeof(NoOpJob));
            detail.Durable = true;
            fJobStore.StoreJob(detail, false);
            fJobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(jobGroup));

            detail = new JobDetailImpl(jobName2, jobGroup, typeof(NoOpJob));
            detail.Durable = true;
            fJobStore.StoreJob(detail, false);

            var trName = "PauseJobGroupPausesNewJobTrigger";
            var trGroup = "PauseJobGroupPausesNewJobTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.UtcNow);
            tr.JobKey = new JobKey(jobName2, jobGroup);
            fJobStore.StoreTrigger(tr, false);
            Assert.AreEqual(TriggerState.Paused, fJobStore.GetTriggerState(tr.Key));
        }

        [Test]
        public async Task TestRetrieveJob_NoJobFound()
        {
            InitJobStore();

            var store = new RavenJobStore();
            var job = await store.RetrieveJob(new JobKey("not", "existing"));
            Assert.IsNull(job);
        }

        [Test]
        public async Task TestRetrieveTrigger_NoTriggerFound()
        {
            InitJobStore();

            var store = new RavenJobStore();
            var trigger = await store.RetrieveTrigger(new TriggerKey("not", "existing"));
            Assert.IsNull(trigger);
        }

        [Test]
        public async Task TestStoreAndRetrieveJobs()
        {
            InitJobStore();

            var store = new RavenJobStore();

            // Store jobs.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await store.StoreJob(job, false);
            }

            // Retrieve jobs.
            for (var i = 0; i < 10; i++)
            {
                var jobKey = JobKey.Create("job" + i);
                var storedJob = await store.RetrieveJob(jobKey);
                Assert.AreEqual(jobKey, storedJob.Key);
            }
        }

        [Test]
        public async Task TestStoreAndRetrieveTriggers()
        {
            InitJobStore();

            var store = new RavenJobStore();
            await store.SchedulerStarted();

            // Store jobs and triggers.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await store.StoreJob(job, true);
                var schedule = SimpleScheduleBuilder.Create();
                var trigger = TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job)
                    .Build();
                await store.StoreTrigger((IOperableTrigger) trigger, true);
            }

            // Retrieve job and trigger.
            for (var i = 0; i < 10; i++)
            {
                var jobKey = JobKey.Create("job" + i);
                var storedJob = await store.RetrieveJob(jobKey);
                Assert.AreEqual(jobKey, storedJob.Key);

                var triggerKey = new TriggerKey("trigger" + i);
                ITrigger storedTrigger = await store.RetrieveTrigger(triggerKey);
                Assert.AreEqual(triggerKey, storedTrigger.Key);
            }
        }

        [Test]
        public async Task TestAcquireTriggers()
        {
            InitJobStore();

            ISchedulerSignaler schedSignaler = new SampleSignaler();
            ITypeLoadHelper loadHelper = new SimpleTypeLoadHelper();
            loadHelper.Initialize();

            var store = new RavenJobStore();
            await store.Initialize(loadHelper, schedSignaler);
            await store.SchedulerStarted();

            // Setup: Store jobs and triggers.
            var startTime0 = DateTime.UtcNow.AddMinutes(1).ToUniversalTime(); // a min from now.
            for (var i = 0; i < 10; i++)
            {
                var startTime = startTime0.AddMinutes(i * 1); // a min apart
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                var schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                var trigger = (IOperableTrigger) TriggerBuilder.Create().WithIdentity("trigger" + i)
                    .WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                var fireTime = trigger.ComputeFirstFireTimeUtc(null);
                Assert.AreEqual(true, fireTime != null);

                await store.StoreJobAndTrigger(job, trigger);
            }

            // Test acquire one trigger at a time
            for (var i = 0; i < 10; i++)
            {
                DateTimeOffset noLaterThan = startTime0.AddMinutes(i);
                var maxCount = 1;
                var timeWindow = TimeSpan.Zero;
                var triggers = (await store.AcquireNextTriggers(noLaterThan, maxCount, timeWindow)).ToList();
                Assert.AreEqual(1, triggers.Count);
                Assert.AreEqual("trigger" + i, triggers[0].Key.Name);

                // Let's remove the trigger now.
                await store.RemoveJob(triggers[0].JobKey);
            }
        }

        [Test]
        public async Task TestAcquireTriggersInBatch()
        {
            InitJobStore();

            ISchedulerSignaler schedSignaler = new SampleSignaler();
            ITypeLoadHelper loadHelper = new SimpleTypeLoadHelper();
            loadHelper.Initialize();

            var store = new RavenJobStore();
            await store.Initialize(loadHelper, schedSignaler);

            // Setup: Store jobs and triggers.
            var startTime0 = DateTimeOffset.UtcNow.AddMinutes(1); // a min from now.
            for (var i = 0; i < 10; i++)
            {
                var startTime = startTime0.AddMinutes(i); // a min apart
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                var schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                var trigger = (IOperableTrigger) TriggerBuilder.Create().WithIdentity("trigger" + i)
                    .WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                var fireTime = trigger.ComputeFirstFireTimeUtc(null);
                Assert.AreEqual(true, fireTime != null);

                await store.StoreJobAndTrigger(job, trigger);
            }

            // Test acquire batch of triggers at a time
            var noLaterThan = startTime0.AddMinutes(10);
            var maxCount = 7;
            var timeWindow = TimeSpan.FromMinutes(8);
            IList<IOperableTrigger> triggers =
                (await store.AcquireNextTriggers(noLaterThan, maxCount, timeWindow)).ToList();
            Assert.AreEqual(7, triggers.Count);
            for (var i = 0; i < 7; i++) Assert.AreEqual("trigger" + i, triggers[i].Key.Name);
        }

        [Test]
        public async Task TestBasicStorageFunctions()
        {
            var sched = await CreateScheduler("TestBasicStorageFunctions", 2);
            await sched.Start();

            // test basic storage functions of scheduler...

            var job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.IsFalse(await sched.CheckExists(new JobKey("j1")), "Unexpected existence of job named 'j1'.");

            await sched.AddJob(job, false);

            Assert.IsTrue(await sched.CheckExists(new JobKey("j1")),
                "Expected existence of job named 'j1' but checkExists return false.");

            job = await sched.GetJobDetail(new JobKey("j1"));

            Assert.IsNotNull(job, "Stored job not found!");

            await sched.DeleteJob(new JobKey("j1"));

            var trigger = TriggerBuilder.Create()
                .WithIdentity("t1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            Assert.IsFalse(await sched.CheckExists(new TriggerKey("t1")),
                "Unexpected existence of trigger named '11'.");

            await sched.ScheduleJob(job, trigger);

            Assert.IsTrue(await sched.CheckExists(new TriggerKey("t1")),
                "Expected existence of trigger named 't1' but checkExists return false.");

            job = await sched.GetJobDetail(new JobKey("j1"));

            Assert.IsNotNull(job, "Stored job not found!");

            trigger = await sched.GetTrigger(new TriggerKey("t1"));

            Assert.IsNotNull(trigger, "Stored trigger not found!");

            job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j2", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t2", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.ScheduleJob(job, trigger);

            job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j3", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t3", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.ScheduleJob(job, trigger);


            IList<string> jobGroups = (await sched.GetJobGroupNames()).ToList();
            IList<string> triggerGroups = (await sched.GetTriggerGroupNames()).ToList();

            Assert.AreEqual(2, jobGroups.Count, "Job group list size expected to be = 2 ");
            Assert.AreEqual(2, triggerGroups.Count, "Trigger group list size expected to be = 2 ");

            ISet<JobKey> jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup)))
                .ToHashSet();
            ISet<TriggerKey> triggerKeys =
                (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup))).ToHashSet();

            Assert.AreEqual(1, jobKeys.Count, "Number of jobs expected in default group was 1 ");
            Assert.AreEqual(1, triggerKeys.Count, "Number of triggers expected in default group was 1 ");

            jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"))).ToHashSet();
            triggerKeys = (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"))).ToHashSet();

            Assert.AreEqual(2, jobKeys.Count, "Number of jobs expected in 'g1' group was 2 ");
            Assert.AreEqual(2, triggerKeys.Count, "Number of triggers expected in 'g1' group was 2 ");


            var s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            await sched.PauseTrigger(new TriggerKey("t2", "g1"));
            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Paused, s, "State of trigger t2 expected to be PAUSED ");

            await sched.ResumeTrigger(new TriggerKey("t2", "g1"));
            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            ISet<string> pausedGroups = (await sched.GetPausedTriggerGroups()).ToHashSet();
            Assert.AreEqual(0, pausedGroups.Count, "Size of paused trigger groups list expected to be 0 ");

            await sched.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            // test that adding a trigger to a paused group causes the new trigger to be paused also... 
            job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j4", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t4", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.ScheduleJob(job, trigger);

            pausedGroups = (await sched.GetPausedTriggerGroups()).ToHashSet();
            Assert.AreEqual(1, pausedGroups.Count, "Size of paused trigger groups list expected to be 1 ");

            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Paused, s, "State of trigger t2 expected to be PAUSED ");

            s = await sched.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.AreEqual(TriggerState.Paused, s, "State of trigger t4 expected to be PAUSED");

            await sched.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            s = await sched.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            pausedGroups = (await sched.GetPausedTriggerGroups()).ToHashSet();
            Assert.AreEqual(0, pausedGroups.Count, "Size of paused trigger groups list expected to be 0 ");


            Assert.IsFalse(await sched.UnscheduleJob(new TriggerKey("foasldfksajdflk")),
                "Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ");

            Assert.IsTrue(await sched.UnscheduleJob(new TriggerKey("t3", "g1")),
                "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"))).ToHashSet();
            triggerKeys = (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"))).ToHashSet();

            Assert.AreEqual(2, jobKeys.Count,
                "Number of jobs expected in 'g1' group was 2 "); // job should have been deleted also, because it is non-durable
            Assert.AreEqual(2, triggerKeys.Count, "Number of triggers expected in 'g1' group was 2 ");

            Assert.IsTrue(await sched.UnscheduleJob(new TriggerKey("t1")),
                "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup))).ToHashSet();
            triggerKeys = (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup)))
                .ToHashSet();

            Assert.AreEqual(1, jobKeys.Count,
                "Number of jobs expected in default group was 1 "); // job should have been left in place, because it is non-durable
            Assert.AreEqual(0, triggerKeys.Count, "Number of triggers expected in default group was 0 ");

            await sched.Shutdown();
        }

        private async Task<IScheduler> CreateScheduler(string name, int threadCount)
        {
            var properties = new NameValueCollection
            {
                // Setting some scheduler properties
                ["quartz.scheduler.instanceName"] = name + "Scheduler",
                ["quartz.scheduler.instanceId"] = "AUTO",
                ["quartz.threadPool.threadCount"] = threadCount.ToString(CultureInfo.InvariantCulture),
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                // Setting RavenDB as the persisted JobStore
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB"
            };

            return await new StdSchedulerFactory(properties).GetScheduler();
        }

        private const string Barrier = "BARRIER";
        private const string DateStamps = "DATE_STAMPS";
        [DisallowConcurrentExecution]
        [PersistJobDataAfterExecution]
        public class TestStatefulJob : IJob
        {
            public Task Execute(IJobExecutionContext context)
            {
                return Task.CompletedTask;
            }
        }

        public class TestJob : IJob
        {
            public Task Execute(IJobExecutionContext context)
            {
                return Task.CompletedTask;
            }
        }

        private static readonly TimeSpan testTimeout = TimeSpan.FromSeconds(125);

        public class TestJobWithSync : IJob
        {
            public Task  Execute(IJobExecutionContext context)
            {
                try
                {
                    List<DateTime> jobExecTimestamps = (List<DateTime>) context.Scheduler.Context.Get(DateStamps);
                    Barrier barrier = (Barrier) context.Scheduler.Context.Get(Barrier);

                    jobExecTimestamps.Add(DateTime.UtcNow);

                    barrier.SignalAndWait(testTimeout);
                }
                catch (Exception e)
                {
                    Console.Write(e);
                    Assert.Fail("Await on barrier was interrupted: " + e);
                }

                return Task.CompletedTask;
            }
        }

        [DisallowConcurrentExecution]
        [PersistJobDataAfterExecution]
        public class TestAnnotatedJob : IJob
        {
            public Task Execute(IJobExecutionContext context)
            {
                return Task.CompletedTask;
            }
        }

        [Test]
        public async Task TestAbilityToFireImmediatelyWhenStartedBefore()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            var sched = await CreateScheduler("testAbilityToFireImmediatelyWhenStartedBefore", 5);
            sched.Context.Put(Barrier, barrier);
            sched.Context.Put(DateStamps, jobExecTimestamps);
            await sched.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<TestJobWithSync>()
                .WithIdentity("job1")
                .Build();

            var trigger1 = TriggerBuilder.Create()
                .ForJob(job1)
                .Build();

            var sTime = DateTime.UtcNow;

            await sched.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(testTimeout);

            await sched.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time.");
        }

        [Test]
        public async Task TestAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            var sched = await CreateScheduler("testAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob", 5);
            await sched.Clear();

            sched.Context.Put(Barrier, barrier);
            sched.Context.Put(DateStamps, jobExecTimestamps);

            await sched.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<TestJobWithSync>()
                .WithIdentity("job1").StoreDurably().Build();
            await sched.AddJob(job1, false);

            var sTime = DateTime.UtcNow;

            await sched.TriggerJob(job1.Key);

            barrier.SignalAndWait(testTimeout);

            await sched.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time."); // This is dangerously subjective!  but what else to do?
        }

        [Test]
        public async Task TestAbilityToFireImmediatelyWhenStartedAfter()
        {
            var jobExecTimestamps = new List<DateTime>();

            var barrier = new Barrier(2);

            var sched = await CreateScheduler("testAbilityToFireImmediatelyWhenStartedAfter", 5);

            sched.Context.Put(Barrier, barrier);
            sched.Context.Put(DateStamps, jobExecTimestamps);

            var job1 = JobBuilder.Create<TestJobWithSync>().WithIdentity("job1").Build();
            var trigger1 = TriggerBuilder.Create().ForJob(job1).Build();

            var sTime = DateTime.UtcNow;

            await sched.Start();
            await sched.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(testTimeout);

            await sched.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000),
                "Immediate trigger did not fire within a reasonable amount of time."); // This is dangerously subjective!  but what else to do?
        }

        [Test]
        public async Task TestScheduleMultipleTriggersForAJob()
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("job1", "group1").Build();
            var trigger1 = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();
            var trigger2 = TriggerBuilder.Create()
                .WithIdentity("trigger2", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();

            ISet<ITrigger> triggersForJob = new HashSet<ITrigger>();
            triggersForJob.Add(trigger1);
            triggersForJob.Add(trigger2);

            var sched = await CreateScheduler("testScheduleMultipleTriggersForAJob", 5);
            await sched.Start();

            await sched.ScheduleJob(job, triggersForJob.ToList(), true);

            IList<ITrigger> triggersOfJob = (await sched.GetTriggersOfJob(job.Key)).ToList();
            Assert.That(triggersOfJob.Count, Is.EqualTo(2));
            Assert.That(triggersOfJob.Contains(trigger1));
            Assert.That(triggersOfJob.Contains(trigger2));

            await sched.Shutdown(false);
        }

        [Test]
        public async Task TestDurableStorageFunctions()
        {
            var sched = await CreateScheduler("testDurableStorageFunctions", 2);
            await sched.Clear();

            // test basic storage functions of scheduler...

            var job = JobBuilder.Create<TestJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            Assert.That(sched.CheckExists(new JobKey("j1")), Is.False, "Unexpected existence of job named 'j1'.");

            await sched.AddJob(job, false);

            Assert.That(await sched.CheckExists(new JobKey("j1")), "Unexpected non-existence of job named 'j1'.");

            var nonDurableJob = JobBuilder.Create<TestJob>()
                .WithIdentity("j2")
                .Build();

            try
            {
                await sched.AddJob(nonDurableJob, false);
                Assert.Fail("Storage of non-durable job should not have succeeded.");
            }
            catch (SchedulerException)
            {
                Assert.That(sched.CheckExists(new JobKey("j2")), Is.False, "Unexpected existence of job named 'j2'.");
            }

            await sched.AddJob(nonDurableJob, false, true);

            Assert.That(await sched.CheckExists(new JobKey("j2")), "Unexpected non-existence of job named 'j2'.");
        }

        [Test]
        public async Task TestShutdownWithoutWaitIsUnclean()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            var scheduler = await CreateScheduler("testShutdownWithoutWaitIsUnclean", 8);
            try
            {
                scheduler.Context.Put(Barrier, barrier);
                scheduler.Context.Put(DateStamps, jobExecTimestamps);
                await scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(
                    JobBuilder.Create<TestJobWithSync>().WithIdentity(jobName).StoreDurably().Build(), false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0) Thread.Sleep(50);
            }
            finally
            {
                await scheduler.Shutdown(false);
            }

            barrier.SignalAndWait(testTimeout);
        }

        [Test]
        public async Task TestShutdownWithWaitIsClean()
        {
            var shutdown = false;
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            var scheduler = await CreateScheduler("testShutdownWithoutWaitIsUnclean", 8);
            try
            {
                scheduler.Context.Put(Barrier, barrier);
                scheduler.Context.Put(DateStamps, jobExecTimestamps);
                await scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(
                    JobBuilder.Create<TestJobWithSync>().WithIdentity(jobName).StoreDurably().Build(), false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0) Thread.Sleep(50);
            }
            finally
            {
                void ThreadStart()
                {
                    try
                    {
                        scheduler.Shutdown(true);
                        shutdown = true;
                    }
                    catch (SchedulerException ex)
                    {
                        throw new Exception("exception: " + ex.Message, ex);
                    }
                }

                var t = new Thread((ThreadStart) ThreadStart);
                t.Start();
                Thread.Sleep(1000);
                Assert.That(shutdown, Is.False);
                barrier.SignalAndWait(testTimeout);
                t.Join();
            }
        }

        public class SampleSignaler : ISchedulerSignaler
        {
            internal int fMisfireCount;

            public Task NotifyTriggerListenersMisfired(ITrigger trigger, CancellationToken cancellationToken = default)
            {
                fMisfireCount++;
                return Task.CompletedTask;
            }

            public Task NotifySchedulerListenersFinalized(ITrigger trigger,
                CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }

            public Task NotifySchedulerListenersJobDeleted(JobKey jobKey, CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }

            public void SignalSchedulingChange(DateTimeOffset? candidateNewNextFireTimeUtc,
                CancellationToken cancellationToken = default)
            {
            }

            public Task NotifySchedulerListenersError(string message, SchedulerException jpe,
                CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }
        }
    }
}