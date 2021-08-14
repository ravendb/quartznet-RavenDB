using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Quartz.Impl.RavenDB.WorkerExample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>();

                    services.AddQuartz(q =>
                        {
                            q.UseMicrosoftDependencyInjectionJobFactory();

                            q.UseDefaultThreadPool(tp =>
                            {
                                tp.MaxConcurrency = 10;
                            });

                            q.UsePersistentStore(s =>
                            {
                                s.UseProperties = true;
                                s.UseRavenDb(options =>
                                {
                                    options.Database = "QuartzDemo";
                                    options.Urls = new[] { "http://live-test.ravendb.net/" };
                                });
                                s.UseJsonSerializer();
                            });
                        }
                    );

                    services.AddQuartzHostedService(
                        q => q.WaitForJobsToComplete = true);
                });
    }
}
