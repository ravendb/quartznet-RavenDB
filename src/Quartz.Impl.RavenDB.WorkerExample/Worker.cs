using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Quartz.Impl.RavenDB.WorkerExample
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        private readonly ISchedulerFactory _sf;

        public Worker(ILogger<Worker> logger, ISchedulerFactory sf)
        {
            _logger = logger;
            _sf = sf;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

            var scheduler = await _sf.GetScheduler(stoppingToken);

            var emptyFridgeJob = JobBuilder.Create<EmptyFridge>()
                .WithIdentity("EmptyFridgeJob", "Office")
                .RequestRecovery()
                .Build();

            var turnOffLightsJob = JobBuilder.Create<TurnOffLights>()
                .WithIdentity("TurnOffLightsJob", "Office")
                .RequestRecovery()
                .Build();

            var checkAliveJob = JobBuilder.Create<CheckAlive>()
                .WithIdentity("CheckAliveJob", "Office")
                .RequestRecovery()
                .Build();

            var visitJob = JobBuilder.Create<Visit>()
                .WithIdentity("VisitJob", "Office")
                .RequestRecovery()
                .Build();

            // Weekly, Friday at 10 AM (Cron Trigger)
            var emptyFridgeTrigger = TriggerBuilder.Create()
                .WithIdentity("EmptyFridge", "Office")
                .WithCronSchedule("0 0 10 ? * FRI")
                .ForJob("EmptyFridgeJob", "Office")
                .Build();

            // Daily at 6 PM (Daily Interval Trigger)
            var turnOffLightsTrigger = TriggerBuilder.Create()
                .WithIdentity("TurnOffLights", "Office")
                .WithDailyTimeIntervalSchedule(s => s
                    .WithIntervalInHours(24)
                    .OnEveryDay()
                    .StartingDailyAt(TimeOfDay.HourAndMinuteOfDay(18, 0)))
                .Build();

            // Periodic check every 10 seconds (Simple Trigger)
            var checkAliveTrigger = TriggerBuilder.Create()
                .WithIdentity("CheckAlive", "Office")
                .StartAt(DateTime.UtcNow.AddSeconds(3))
                .WithSimpleSchedule(x => x
                    .WithIntervalInSeconds(10)
                    .RepeatForever())
                .Build();

            var visitTrigger = TriggerBuilder.Create()
                .WithIdentity("Visit", "Office")
                .StartAt(DateTime.UtcNow.AddSeconds(3))
                .Build();
            
            await scheduler.ScheduleJob(checkAliveJob, checkAliveTrigger, stoppingToken);
            await scheduler.ScheduleJob(emptyFridgeJob, emptyFridgeTrigger, stoppingToken);
            await scheduler.ScheduleJob(turnOffLightsJob, turnOffLightsTrigger, stoppingToken);
            await scheduler.ScheduleJob(visitJob, visitTrigger, stoppingToken);

            while (!stoppingToken.IsCancellationRequested) await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }
    }
}