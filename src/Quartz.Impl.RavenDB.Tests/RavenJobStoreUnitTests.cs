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
using Quartz.Job;
using Quartz.Simpl;
using Quartz.Spi;
using System.Collections.Specialized;

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
            fSignaler = new SampleSignaler();
            fJobStore.Initialize(null, fSignaler);
            fJobStore.SchedulerStarted();

            fJobDetail = new JobDetailImpl("job1", "jobGroup1", typeof (NoOpJob));
            fJobDetail.Durable = true;
            fJobStore.StoreJob(fJobDetail, true);
        }

        [Test]
        public void TestAcquireNextTrigger()
        {
            DateTimeOffset d = DateBuilder.EvenMinuteDateAfterNow();
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddSeconds(200), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddSeconds(50), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger1", "triggerGroup2", fJobDetail.Name, fJobDetail.Group, d.AddSeconds(100), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));

            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            fJobStore.StoreTrigger(trigger1, false);
            fJobStore.StoreTrigger(trigger2, false);
            fJobStore.StoreTrigger(trigger3, false);

            DateTimeOffset firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            Assert.AreEqual(0, fJobStore.AcquireNextTriggers(d.AddMilliseconds(10), 1, TimeSpan.Zero).Count);
            Assert.AreEqual(trigger2, fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)[0]);
            Assert.AreEqual(trigger3, fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)[0]);
            Assert.AreEqual(trigger1, fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero)[0]);
            Assert.AreEqual(0, fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.Zero).Count);


            // release trigger3
            fJobStore.ReleaseAcquiredTrigger(trigger3);
            Assert.AreEqual(trigger3, fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1))[0]);
        }

        [Test]
        public void TestAcquireNextTriggerBatch()
        {
            DateTimeOffset d = DateBuilder.EvenMinuteDateAfterNow();
            
            IOperableTrigger early = new SimpleTriggerImpl("early", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d, d.AddMilliseconds(5), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200000), d.AddMilliseconds(200005), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200100), d.AddMilliseconds(200105), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger3", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200200), d.AddMilliseconds(200205), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger4 = new SimpleTriggerImpl("trigger4", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(200300), d.AddMilliseconds(200305), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger10 = new SimpleTriggerImpl("trigger10", "triggerGroup2", fJobDetail.Name, fJobDetail.Group, d.AddMilliseconds(500000), d.AddMilliseconds(700000), 2, TimeSpan.FromSeconds(2));

            early.ComputeFirstFireTimeUtc(null);
            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            trigger4.ComputeFirstFireTimeUtc(null);
            trigger10.ComputeFirstFireTimeUtc(null);
            fJobStore.StoreTrigger(early, false);
            fJobStore.StoreTrigger(trigger1, false);
            fJobStore.StoreTrigger(trigger2, false);
            fJobStore.StoreTrigger(trigger3, false);
            fJobStore.StoreTrigger(trigger4, false);
            fJobStore.StoreTrigger(trigger10, false);

            DateTimeOffset firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            IList<IOperableTrigger> acquiredTriggers = fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 4, TimeSpan.FromSeconds(1));
            Assert.AreEqual(4, acquiredTriggers.Count);
            Assert.AreEqual(early.Key, acquiredTriggers[0].Key);
            Assert.AreEqual(trigger1.Key, acquiredTriggers[1].Key);
            Assert.AreEqual(trigger2.Key, acquiredTriggers[2].Key);
            Assert.AreEqual(trigger3.Key, acquiredTriggers[3].Key);
            fJobStore.ReleaseAcquiredTrigger(early);
      		fJobStore.ReleaseAcquiredTrigger(trigger1);
        	fJobStore.ReleaseAcquiredTrigger(trigger2);
        	fJobStore.ReleaseAcquiredTrigger(trigger3);
			
            acquiredTriggers = this.fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 5, TimeSpan.FromMilliseconds(1000));
            Assert.AreEqual(5, acquiredTriggers.Count);
            Assert.AreEqual(early.Key, acquiredTriggers[0].Key);
            Assert.AreEqual(trigger1.Key, acquiredTriggers[1].Key);
            Assert.AreEqual(trigger2.Key, acquiredTriggers[2].Key);
            Assert.AreEqual(trigger3.Key, acquiredTriggers[3].Key);
            Assert.AreEqual(trigger4.Key, acquiredTriggers[4].Key);
            fJobStore.ReleaseAcquiredTrigger(early);
            fJobStore.ReleaseAcquiredTrigger(trigger1);
            fJobStore.ReleaseAcquiredTrigger(trigger2);
            fJobStore.ReleaseAcquiredTrigger(trigger3);
            fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 6, TimeSpan.FromSeconds(1));
            Assert.AreEqual(5, acquiredTriggers.Count);
            Assert.AreEqual(early.Key, acquiredTriggers[0].Key);
            Assert.AreEqual(trigger1.Key, acquiredTriggers[1].Key);
            Assert.AreEqual(trigger2.Key, acquiredTriggers[2].Key);
            Assert.AreEqual(trigger3.Key, acquiredTriggers[3].Key);
            Assert.AreEqual(trigger4.Key, acquiredTriggers[4].Key);
            fJobStore.ReleaseAcquiredTrigger(early);
            fJobStore.ReleaseAcquiredTrigger(trigger1);
            fJobStore.ReleaseAcquiredTrigger(trigger2);
            fJobStore.ReleaseAcquiredTrigger(trigger3);
            fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(1), 5, TimeSpan.Zero);
            Assert.AreEqual(2, acquiredTriggers.Count);
            fJobStore.ReleaseAcquiredTrigger(early);
            fJobStore.ReleaseAcquiredTrigger(trigger1);

            acquiredTriggers = fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(250), 5, TimeSpan.FromMilliseconds(199));
            Assert.AreEqual(5, acquiredTriggers.Count);
            fJobStore.ReleaseAcquiredTrigger(early); 
            fJobStore.ReleaseAcquiredTrigger(trigger1);
            fJobStore.ReleaseAcquiredTrigger(trigger2);
            fJobStore.ReleaseAcquiredTrigger(trigger3);
            fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers = fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(150), 5, TimeSpan.FromMilliseconds(50L));
            Assert.AreEqual(4, acquiredTriggers.Count);
            fJobStore.ReleaseAcquiredTrigger(early);
            fJobStore.ReleaseAcquiredTrigger(trigger1);
            fJobStore.ReleaseAcquiredTrigger(trigger2);
            fJobStore.ReleaseAcquiredTrigger(trigger3);
        }

        [Test]
        public void TestTriggerStates()
        {
            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            Assert.AreEqual(TriggerState.None, fJobStore.GetTriggerState(trigger.Key));
            fJobStore.StoreTrigger(trigger, false);
            Assert.AreEqual(TriggerState.Normal, fJobStore.GetTriggerState(trigger.Key));

            fJobStore.PauseTrigger(trigger.Key);
            Assert.AreEqual(TriggerState.Paused, fJobStore.GetTriggerState(trigger.Key));

            fJobStore.ResumeTrigger(trigger.Key);
            Assert.AreEqual(TriggerState.Normal, fJobStore.GetTriggerState(trigger.Key));

            trigger = fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1))[0];
            Assert.IsNotNull(trigger);
            fJobStore.ReleaseAcquiredTrigger(trigger);
            trigger = fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1))[0];
            Assert.IsNotNull(trigger);
            Assert.AreEqual(0, fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1, TimeSpan.FromMilliseconds(1)).Count);
        }

        [Test]
        public void TestRemoveCalendarWhenTriggersPresent()
        {
            // QRTZNET-29

            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", fJobDetail.Name, fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            ICalendar cal = new MonthlyCalendar();
            fJobStore.StoreTrigger(trigger, false);
            fJobStore.StoreCalendar("cal", cal, false, true);

            fJobStore.RemoveCalendar("cal");
        }

        [Test]
        public void TestStoreTriggerReplacesTrigger()
        {
            string jobName = "StoreJobReplacesJob";
            string jobGroup = "StoreJobReplacesJobGroup";
            JobDetailImpl detail = new JobDetailImpl(jobName, jobGroup, typeof (NoOpJob));
            fJobStore.StoreJob(detail, false);

            string trName = "StoreTriggerReplacesTrigger";
            string trGroup = "StoreTriggerReplacesTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.Now);
            tr.JobKey = new JobKey(jobName, jobGroup);
            tr.CalendarName = null;

            fJobStore.StoreTrigger(tr, false);
            Assert.AreEqual(tr, fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)));

            tr.CalendarName = "NonExistingCalendar";
            fJobStore.StoreTrigger(tr, true);
            Assert.AreEqual(tr, fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)));
            Assert.AreEqual(tr.CalendarName, fJobStore.RetrieveTrigger(new TriggerKey(trName, trGroup)).CalendarName, "StoreJob doesn't replace triggers");

            bool exceptionRaised = false;
            try
            {
                fJobStore.StoreTrigger(tr, false);
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
            string jobName1 = "PauseJobGroupPausesNewJob";
            string jobName2 = "PauseJobGroupPausesNewJob2";
            string jobGroup = "PauseJobGroupPausesNewJobGroup";
            JobDetailImpl detail = new JobDetailImpl(jobName1, jobGroup, typeof (NoOpJob));
            detail.Durable = true;
            fJobStore.StoreJob(detail, false);
            fJobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(jobGroup));

            detail = new JobDetailImpl(jobName2, jobGroup, typeof (NoOpJob));
            detail.Durable = true;
            fJobStore.StoreJob(detail, false);

            string trName = "PauseJobGroupPausesNewJobTrigger";
            string trGroup = "PauseJobGroupPausesNewJobTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.UtcNow);
            tr.JobKey = new JobKey(jobName2, jobGroup);
            fJobStore.StoreTrigger(tr, false);
            Assert.AreEqual(TriggerState.Paused, fJobStore.GetTriggerState(tr.Key));
        }

        [Test]
        public void TestRetrieveJob_NoJobFound()
        {
            var store = new RavenJobStore();
            IJobDetail job = store.RetrieveJob(new JobKey("not", "existing"));
            Assert.IsNull(job);
        }

        [Test]
        public void TestRetrieveTrigger_NoTriggerFound()
        {
            var store = new RavenJobStore();
            IOperableTrigger trigger = store.RetrieveTrigger(new TriggerKey("not", "existing"));
            Assert.IsNull(trigger);
        }

        [Test]
        public void testStoreAndRetrieveJobs()
        {
            var store = new RavenJobStore();

            // Store jobs.
            for (int i = 0; i < 10; i++)
            {
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                store.StoreJob(job, false);
            }
            // Retrieve jobs.
            for (int i = 0; i < 10; i++)
            {
                JobKey jobKey = JobKey.Create("job" + i);
                IJobDetail storedJob = store.RetrieveJob(jobKey);
                Assert.AreEqual(jobKey, storedJob.Key);
            }
        }

        [Test]
        public void TestStoreAndRetrieveTriggers()
        {
            var store = new RavenJobStore();
            store.SchedulerStarted();

            // Store jobs and triggers.
            for (int i = 0; i < 10; i++)
            {
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                store.StoreJob(job, true);
                SimpleScheduleBuilder schedule = SimpleScheduleBuilder.Create();
                ITrigger trigger = TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job).Build();
                store.StoreTrigger((IOperableTrigger) trigger, true);
            }
            // Retrieve job and trigger.
            for (int i = 0; i < 10; i++)
            {
                JobKey jobKey = JobKey.Create("job" + i);
                IJobDetail storedJob = store.RetrieveJob(jobKey);
                Assert.AreEqual(jobKey, storedJob.Key);

                TriggerKey triggerKey = new TriggerKey("trigger" + i);
                ITrigger storedTrigger = store.RetrieveTrigger(triggerKey);
                Assert.AreEqual(triggerKey, storedTrigger.Key);
            }
        }

        [Test]
        public void TestAcquireTriggers()
        {
            ISchedulerSignaler schedSignaler = new SampleSignaler();
            ITypeLoadHelper loadHelper = new SimpleTypeLoadHelper();
            loadHelper.Initialize();

            var store = new RavenJobStore();
            store.Initialize(loadHelper, schedSignaler);
            store.SchedulerStarted();

            // Setup: Store jobs and triggers.
            DateTime startTime0 = DateTime.UtcNow.AddMinutes(1).ToUniversalTime(); // a min from now.
            for (int i = 0; i < 10; i++)
            {
                DateTime startTime = startTime0.AddMinutes(i*1); // a min apart
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                SimpleScheduleBuilder schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                IOperableTrigger trigger = (IOperableTrigger) TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                DateTimeOffset? fireTime = trigger.ComputeFirstFireTimeUtc(null);
                Assert.AreEqual(true, fireTime != null);

                store.StoreJobAndTrigger(job, trigger);
            }

            // Test acquire one trigger at a time
            for (int i = 0; i < 10; i++)
            {
                DateTimeOffset noLaterThan = startTime0.AddMinutes(i);
                int maxCount = 1;
                TimeSpan timeWindow = TimeSpan.Zero;
                IList<IOperableTrigger> triggers = store.AcquireNextTriggers(noLaterThan, maxCount, timeWindow);
                Assert.AreEqual(1, triggers.Count);
                Assert.AreEqual("trigger" + i, triggers[0].Key.Name);

                // Let's remove the trigger now.
                store.RemoveJob(triggers[0].JobKey);
            }
        }

        [Test]
        public void TestAcquireTriggersInBatch()
        {
            ISchedulerSignaler schedSignaler = new SampleSignaler();
            ITypeLoadHelper loadHelper = new SimpleTypeLoadHelper();
            loadHelper.Initialize();

            var store = new RavenJobStore();
            store.Initialize(loadHelper, schedSignaler);

            // Setup: Store jobs and triggers.
            DateTimeOffset startTime0 = DateTimeOffset.UtcNow.AddMinutes(1); // a min from now.
            for (int i = 0; i < 10; i++)
            {
                DateTimeOffset startTime = startTime0.AddMinutes(i); // a min apart
                IJobDetail job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                SimpleScheduleBuilder schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                IOperableTrigger trigger = (IOperableTrigger) TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                DateTimeOffset? fireTime = trigger.ComputeFirstFireTimeUtc(null);
                Assert.AreEqual(true, fireTime != null);

                store.StoreJobAndTrigger(job, trigger);
            }

            // Test acquire batch of triggers at a time
            DateTimeOffset noLaterThan = startTime0.AddMinutes(10);
            int maxCount = 7;
            TimeSpan timeWindow = TimeSpan.FromMinutes(8);
            IList<IOperableTrigger> triggers = store.AcquireNextTriggers(noLaterThan, maxCount, timeWindow);
            Assert.AreEqual(7, triggers.Count);
            for (int i = 0; i < 7; i++)
            {
                Assert.AreEqual("trigger" + i, triggers[i].Key.Name);
            }
        }
        public class TestJob : IJob
        {
            public void Execute(IJobExecutionContext context)
            {
            }
        }

        [Test]
        public void TestBasicStorageFunctions()
        {
            fJobStore.ClearAllSchedulingData();

            NameValueCollection properties = new NameValueCollection
            {
                // Setting some scheduler properties
                ["quartz.scheduler.instanceName"] = "SchedulerTest_Scheduler",
                ["quartz.scheduler.instanceId"] = "AUTO",
                ["quartz.threadPool.threadCount"] = "2",
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                // Setting RavenDB as the persisted JobStore
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB",
            };

            IScheduler sched = new StdSchedulerFactory(properties).GetScheduler();
            sched.Start();

            // test basic storage functions of scheduler...

            IJobDetail job = JobBuilder.Create()
                                       .OfType<TestJob>()
                                       .WithIdentity("j1")
                                       .StoreDurably()
                                       .Build();

            Assert.IsFalse(sched.CheckExists(new JobKey("j1")), "Unexpected existence of job named 'j1'.");

            sched.AddJob(job, false);

            Assert.IsTrue(sched.CheckExists(new JobKey("j1")), "Expected existence of job named 'j1' but checkExists return false.");

            job = sched.GetJobDetail(new JobKey("j1"));

            Assert.IsNotNull(job, "Stored job not found!");

            sched.DeleteJob(new JobKey("j1"));

            ITrigger trigger = TriggerBuilder.Create()
                                             .WithIdentity("t1")
                                             .ForJob(job)
                                             .StartNow()
                                             .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                                             .Build();

            Assert.IsFalse(sched.CheckExists(new TriggerKey("t1")), "Unexpected existence of trigger named '11'.");

            sched.ScheduleJob(job, trigger);

            Assert.IsTrue(sched.CheckExists(new TriggerKey("t1")), "Expected existence of trigger named 't1' but checkExists return false.");

            job = sched.GetJobDetail(new JobKey("j1"));

            Assert.IsNotNull(job, "Stored job not found!");

            trigger = sched.GetTrigger(new TriggerKey("t1"));

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

            sched.ScheduleJob(job, trigger);

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

            sched.ScheduleJob(job, trigger);


            IList<string> jobGroups = sched.GetJobGroupNames();
            IList<string> triggerGroups = sched.GetTriggerGroupNames();

            Assert.AreEqual(2, jobGroups.Count, "Job group list size expected to be = 2 ");
            Assert.AreEqual(2, triggerGroups.Count, "Trigger group list size expected to be = 2 ");

            Collection.ISet<JobKey> jobKeys = sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            Collection.ISet<TriggerKey> triggerKeys = sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.AreEqual(1, jobKeys.Count, "Number of jobs expected in default group was 1 ");
            Assert.AreEqual(1, triggerKeys.Count, "Number of triggers expected in default group was 1 ");

            jobKeys = sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            triggerKeys = sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.AreEqual(2, jobKeys.Count, "Number of jobs expected in 'g1' group was 2 ");
            Assert.AreEqual(2, triggerKeys.Count, "Number of triggers expected in 'g1' group was 2 ");


            TriggerState s = sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            sched.PauseTrigger(new TriggerKey("t2", "g1"));
            s = sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Paused, s, "State of trigger t2 expected to be PAUSED ");

            sched.ResumeTrigger(new TriggerKey("t2", "g1"));
            s = sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            Collection.ISet<string> pausedGroups = sched.GetPausedTriggerGroups();
            Assert.AreEqual(0, pausedGroups.Count, "Size of paused trigger groups list expected to be 0 ");

            sched.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

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

            sched.ScheduleJob(job, trigger);

            pausedGroups = sched.GetPausedTriggerGroups();
            Assert.AreEqual(1, pausedGroups.Count, "Size of paused trigger groups list expected to be 1 ");

            s = sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Paused, s, "State of trigger t2 expected to be PAUSED ");

            s = sched.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.AreEqual(TriggerState.Paused, s, "State of trigger t4 expected to be PAUSED");

            sched.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            s = sched.GetTriggerState(new TriggerKey("t2", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            s = sched.GetTriggerState(new TriggerKey("t4", "g1"));
            Assert.AreEqual(TriggerState.Normal, s, "State of trigger t2 expected to be NORMAL ");

            pausedGroups = sched.GetPausedTriggerGroups();
            Assert.AreEqual(0, pausedGroups.Count, "Size of paused trigger groups list expected to be 0 ");


            Assert.IsFalse(sched.UnscheduleJob(new TriggerKey("foasldfksajdflk")), "Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ");

            Assert.IsTrue(sched.UnscheduleJob(new TriggerKey("t3", "g1")), "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
            triggerKeys = sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            Assert.AreEqual(2, jobKeys.Count, "Number of jobs expected in 'g1' group was 2 "); // job should have been deleted also, because it is non-durable
            Assert.AreEqual(2, triggerKeys.Count, "Number of triggers expected in 'g1' group was 2 ");

            Assert.IsTrue(sched.UnscheduleJob(new TriggerKey("t1")), "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
            triggerKeys = sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

            Assert.AreEqual(1, jobKeys.Count, "Number of jobs expected in default group was 1 "); // job should have been left in place, because it is non-durable
            Assert.AreEqual(0, triggerKeys.Count, "Number of triggers expected in default group was 0 ");

            sched.Shutdown();
        }

        public class SampleSignaler : ISchedulerSignaler
        {
            internal int fMisfireCount = 0;

            public void NotifyTriggerListenersMisfired(ITrigger trigger)
            {
                fMisfireCount++;
            }

            public void NotifySchedulerListenersFinalized(ITrigger trigger)
            {
            }

            public void SignalSchedulingChange(DateTimeOffset? candidateNewNextFireTimeUtc)
            {
            }

            public void NotifySchedulerListenersError(string message, SchedulerException jpe)
            {
            }

            public void NotifySchedulerListenersJobDeleted(JobKey jobKey)
            {
            }
        }
    }
}