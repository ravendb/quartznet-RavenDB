using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Quartz.Impl.RavenDB
{
    internal class Job
    {
        public string Name { get; set; }
        public string Group { get; set; }
        public string Key { get; set; }
        public string Scheduler { get; set; }

        public string Description { get; set; }
        public Type JobType { get; set; }
        public bool Durable { get; set; }
        public bool ConcurrentExecutionDisallowed { get; set; }
        public bool PersistJobDataAfterExecution { get; set; }
        public bool RequestsRecovery { get; set; }
        public IDictionary<string, object> JobDataMap { get; set; }

        public Job(IJobDetail newJob, string schedulerInstanceName)
        {
            if (newJob == null) return;
            Name = newJob.Key.Name;
            Group = newJob.Key.Group;
            Key = Name + "/" + Group;
            Scheduler = schedulerInstanceName;

            Description = newJob.Description;
            JobType = newJob.JobType;
            Durable = newJob.Durable;
            ConcurrentExecutionDisallowed = newJob.ConcurrentExecutionDisallowed;
            PersistJobDataAfterExecution = newJob.PersistJobDataAfterExecution;
            RequestsRecovery = newJob.RequestsRecovery;
            JobDataMap = new Dictionary<string, object>(newJob.JobDataMap.WrappedMap);
        }

        public IJobDetail Deserialize()
        {
            return JobBuilder.Create()
                    .WithIdentity(Name, Group)
                    .WithDescription(Description)
                    .OfType(JobType)
                    .RequestRecovery(RequestsRecovery)
                    .SetJobData(new JobDataMap(JobDataMap))
                    .StoreDurably(Durable)
                    .Build();

            // A JobDetail doesn't have builder methods for two properties:   IsNonConcurrent,IsUpdateData
            // they are determined according to attributes on the job class
        }

    }
}