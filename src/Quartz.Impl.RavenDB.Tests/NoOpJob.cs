using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB.Tests
{
    internal class NoOpJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            // NoOp
            return Task.CompletedTask;
        }
    }
}
