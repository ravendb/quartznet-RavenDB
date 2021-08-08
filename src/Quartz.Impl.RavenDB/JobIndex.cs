using Raven.Client.Documents.Indexes;

namespace Quartz.Impl.RavenDB
{
    public class JobIndex : AbstractIndexCreationTask
    {
        public override string IndexName => "JobIndex";

        public override IndexDefinition CreateIndexDefinition()
        {
            var definition = new IndexDefinition();

            definition.Maps.Add(@"from doc in docs.Jobs
                        select new {
	                        Group = doc.Group,
	                        RequestsRecovery = doc.RequestsRecovery,
	                        Scheduler = doc.Scheduler
                        }");

            return definition;
        }
    }
}
