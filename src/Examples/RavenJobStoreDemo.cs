using System;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;
using Quartz;
using Quartz.Impl;

namespace Examples
{
    internal class RavenJobStoreDemo
    {

        private static void Main(string[] args)
        {
            Common.Logging.LogManager.Adapter = new Common.Logging.Simple.ConsoleOutLoggerFactoryAdapter
            {
                Level = Common.Logging.LogLevel.Info
            };

            NameValueCollection properties = new NameValueCollection
            {
                // Setting some scheduler properties
                ["quartz.scheduler.instanceName"] = "QuartzRavenDBDemo",
                ["quartz.scheduler.instanceId"] = "instance_one",
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                ["quartz.threadPool.threadCount"] = "1",
                ["quartz.threadPool.threadPriority"] = "Normal",
                // Setting RavenDB as the persisted JobStore
                ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB",
            };

            try
            {

                ISchedulerFactory sf = new StdSchedulerFactory(properties);
                IScheduler scheduler = sf.GetScheduler().Result;
                scheduler.Start();

                IJobDetail emptyFridgeJob = JobBuilder.Create<EmptyFridge>()
                    .WithIdentity("EmptyFridgeJob", "Office")
                    .Build();

                IJobDetail turnOffLightsJob = JobBuilder.Create<TurnOffLights>()
                    .WithIdentity("TurnOffLightsJob", "Office")
                    .Build();

                IJobDetail checkAliveJob = JobBuilder.Create<CheckAlive>()
                    .WithIdentity("CheckAliveJob", "Office")
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
                ITrigger checkAliveTrigger = TriggerBuilder.Create()
                    .WithIdentity("CheckAlive", "Office")
                    .StartAt(DateTime.UtcNow.AddSeconds(3))
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(10)
                        .RepeatForever())
                    .Build();


                scheduler.ScheduleJob(checkAliveJob, checkAliveTrigger);
                scheduler.ScheduleJob(emptyFridgeJob, emptyFridgeTrigger);
                scheduler.ScheduleJob(turnOffLightsJob, turnOffLightsTrigger);

                // some sleep to show what's happening
                Thread.Sleep(TimeSpan.FromSeconds(600));

                scheduler.Shutdown();
            }
            catch (SchedulerException se)
            {
                Console.WriteLine(se);
            }

            Console.WriteLine("Press any key to close the application");
            Console.ReadKey();
        }
    }

    [PersistJobDataAfterExecution]
    public class EmptyFridge : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Emptying the fridge...");
            return Task.CompletedTask;
        }
    }

    [PersistJobDataAfterExecution]
    public class TurnOffLights : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Turning lights off...");
            return Task.CompletedTask;
        }
    }

    [PersistJobDataAfterExecution]
    public class CheckAlive : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Verifying site is up...");
            return Task.CompletedTask;
        }
    }
}