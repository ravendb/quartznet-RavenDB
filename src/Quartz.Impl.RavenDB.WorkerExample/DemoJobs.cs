using System;
using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB.WorkerExample
{
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

    [PersistJobDataAfterExecution]
    public class Visit : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Visiting the office, once :)");
            return Task.CompletedTask;
        }
    }
}
