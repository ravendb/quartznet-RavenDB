using System;
using System.Collections.Specialized;
using NUnit.Framework;

namespace Quartz.Impl.RavenDB.Tests
{
    public class RavenJobStoreIssueNo5
    {
        private readonly JobListener listener = new JobListener();

        private readonly ITrigger trigger = TriggerBuilder.Create()
            .WithIdentity("trigger", "g1")
            .StartAt(DateTime.Now - TimeSpan.FromHours(1))
            .WithSimpleSchedule(s => s.WithMisfireHandlingInstructionFireNow())
            .Build();

        private readonly IJobDetail job = JobBuilder.Create<TestJob>()
            .WithIdentity("job", "g1")
            .Build();

        private void ScheduleTestJobAndWaitForExecution(IScheduler scheduler)
        {
            scheduler.ListenerManager.AddJobListener(listener);
            scheduler.Start();
            scheduler.ScheduleJob(job, trigger);
            while (!scheduler.CheckExists(job.Key)) ;
            scheduler.Shutdown(true);
        }

        [Test]
        public void InMemory()
        {
            var scheduler = StdSchedulerFactory.GetDefaultScheduler();

            ScheduleTestJobAndWaitForExecution(scheduler);

            Assert.True(listener.WasExecuted);
        }

        [Test]
        public void InRavenDB()
        {
             NameValueCollection properties = new NameValueCollection
            {
                // Normal scheduler properties
                ["quartz.scheduler.instanceName"] = "TestScheduler",
                ["quartz.scheduler.instanceId"] = "instance_one",
                // RavenDB JobStore property
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB"
            };

            ISchedulerFactory sf = new StdSchedulerFactory(properties);
            IScheduler scheduler = sf.GetScheduler();

            ScheduleTestJobAndWaitForExecution(scheduler);

            Assert.True(listener.WasExecuted);
        }
        public class TestJob : IJob
        {
            public void Execute(IJobExecutionContext context)
            {
            }
        }

        public class JobListener : IJobListener
        {
            public void JobToBeExecuted(IJobExecutionContext context)
            {

            }

            public void JobExecutionVetoed(IJobExecutionContext context)
            {

            }

            public void JobWasExecuted(IJobExecutionContext context, JobExecutionException jobException)
            {
                WasExecuted = true;
            }

            public bool WasExecuted { get; private set; }

            public string Name => "JobListener";
        }
    }
}
