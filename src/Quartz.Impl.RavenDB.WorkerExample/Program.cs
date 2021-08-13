using System;
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
                                s.RetryInterval = TimeSpan.FromSeconds(15);
                                s.UseRavenDb();
                                s.UseJsonSerializer();
                                s.UseClustering(c =>
                                {
                                    c.CheckinMisfireThreshold = TimeSpan.FromSeconds(20);
                                    c.CheckinInterval = TimeSpan.FromSeconds(10);
                                });
                            });
                        }
                    );

                    services.AddQuartzHostedService(
                        q => q.WaitForJobsToComplete = true);
                });
    }
}
