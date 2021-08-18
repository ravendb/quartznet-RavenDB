using System;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Quartz.Impl.RavenDB.Tests
{
    public class RavenJobStoreIssueNo5
    {
        private readonly JobListener _listener = new JobListener();

        private readonly ITrigger _trigger = TriggerBuilder.Create()
            .WithIdentity("trigger", "g1")
            .StartAt(DateTime.Now - TimeSpan.FromHours(1))
            .WithSimpleSchedule(s => s.WithMisfireHandlingInstructionFireNow())
            .Build();

        private readonly IJobDetail _job = JobBuilder.Create<TestJob>()
            .WithIdentity("job", "g1")
            .Build();

        private async Task ScheduleTestJobAndWaitForExecution(IScheduler scheduler)
        {
            scheduler.ListenerManager.AddJobListener(_listener);
            await scheduler.Start();
            await scheduler.ScheduleJob(_job, _trigger);
            while (!await scheduler.CheckExists(_job.Key)) ;
            await scheduler.Shutdown(true);
        }

        [Test]
        public async Task InMemory()
        {
            var scheduler = await StdSchedulerFactory.GetDefaultScheduler();

            await ScheduleTestJobAndWaitForExecution(scheduler);

            Assert.True(_listener.WasExecuted);
        }

        [Test]
        public async Task InRavenDb()
        {
            var properties = new NameValueCollection
            {
                // Normal scheduler properties
                ["quartz.scheduler.instanceName"] = "TestScheduler",
                ["quartz.scheduler.instanceId"] = "instance_one",
                // RavenDB JobStore property
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB"
            };

            ISchedulerFactory sf = new StdSchedulerFactory(properties);
            var scheduler = await sf.GetScheduler();

            await ScheduleTestJobAndWaitForExecution(scheduler);

            Assert.True(_listener.WasExecuted);
        }

        public class TestJob : IJob
        {
            public Task Execute(IJobExecutionContext context)
            {
                return Task.CompletedTask;
            }
        }

        public class JobListener : IJobListener
        {
            public Task JobToBeExecuted(IJobExecutionContext context, CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }

            public Task JobExecutionVetoed(IJobExecutionContext context, CancellationToken cancellationToken = default)
            {
                return Task.CompletedTask;
            }

            public Task JobWasExecuted(IJobExecutionContext context, JobExecutionException jobException, CancellationToken cancellationToken = default)
            {
                WasExecuted = true;
                return Task.CompletedTask;
            }

            public bool WasExecuted { get; private set; }

            public string Name => "JobListener";
        }
    }
}
