using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;

namespace Quartz.Impl.RavenDB
{
    public class JobIndex : AbstractIndexCreationTask
    {
        public override string IndexName => "JobIndex";

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Map = @"from doc in docs.Jobs
                        select new {
	                        Group = doc.Group,
	                        RequestsRecovery = doc.RequestsRecovery,
	                        Scheduler = doc.Scheduler
                        }"
            };
        }
    }
}
