# Quartz.NET-RavenDB

JobStore implementation for Quartz.NET scheduler using RavenDB.

## About

[Quartz.NET](https://github.com/quartznet/quartznet) is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

[Quartz.NET on RavenDB](https://github.com/ravendb/quartznet-RavenDB) is a new provider written for Quartz.NET which lets us use the [RavenDB](https://ravendb.net/features) NoSQL database as the persistent Job Store for scheduling data (instead of the SQL solutions that are built-in Quartz.NET).

## Installation

First add scheduling to your app using Quartz.NET ([example](http://www.quartz-scheduler.net/documentation/quartz-2.x/quick-start.html)).
Then install the NuGet [package](https://www.nuget.org/packages/Quartz.Impl.RavenDB/).

## Configuration & Usage

### .NET Framework

In your code, where you would have [normally configured](https://www.quartz-scheduler.net/documentation/quartz-3.x/tutorial/job-stores.html) Quartz to use a persistent job store, you must add the following configuration properties:

```csharp
// In your application where you want to setup the scheduler:
NameValueCollection properties = new NameValueCollection
{
    // Normal scheduler properties
    ["quartz.scheduler.instanceName"] = "TestScheduler",
    ["quartz.scheduler.instanceId"] = "instance_one",
    ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
    ["quartz.threadPool.threadCount"] = "1",
    ["quartz.threadPool.threadPriority"] = "Normal",

    // RavenDB JobStore property
    ["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB"
    // RavenDB Default Database name
    ["quartz.jobStore.database"] = "QuartzDemo",
    // One or more URLs to database server
    ["quartz.jobStore.urls"] = "[\"http://live-test.ravendb.net\"]",
    // If you use authentication, specify certificate and password
    //["quartz.jobStore.certPath"] = "My/Cert/path.pfx",
    //["quartz.jobStore.certPass"] = "SuperSecret",
};

// Init scheduler with the desired configuration properties
ISchedulerFactory sf = new StdSchedulerFactory(properties);
IScheduler scheduler = sf.GetScheduler();
```

### .NET Core

For use in .NET Core Workers or ASP.NET Core use the recommended Fluent API when setting up a Quartz in `ConfigureServices` like:

```csharp
var configuration = new ConfigurationBuilder()
                        .AddJsonFile("appsettings.json")
                        .Build();

var section = configuration.GetSection("MyService");
var config = section.Get<MyServiceConfig>();

services.AddQuartz(q =>
    {
        q.UseMicrosoftDependencyInjectionJobFactory();

        q.UseDefaultThreadPool(tp =>
        {
            tp.MaxConcurrency = 10;
        });

        q.UsePersistentStore(s =>
        {
            s.UseRavenDb(options =>
            {
                options.Urls = config.QuartzRavenStore.Urls;
                options.Database = config.QuartzRavenStore.DatabaseName;
                // specify certificate, if necessary
            });

            s.UseJsonSerializer();
        });
        
        // Add jobs and triggers as you wish
    }
);

services.AddQuartzHostedService(
    q => q.WaitForJobsToComplete = true);

```
