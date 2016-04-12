# Quartz.NET-RavenDB
JobStore implementation for Quartz.NET scheduler using RavenDB.

###### About
[Quartz.NET](http://www.quartz-scheduler.net/) is a full-featured, open source job scheduling service that can be integrated with, or used along side virtually any .Net application - from the smallest stand-alone application to the largest e-commerce system. Quartz.Net can be used to create simple or complex schedules for executing tens, hundreds, or even tens-of-thousands of jobs; jobs whose tasks are defined as standard .Net components that are programmed to fulfill the requirements of your application.

[Quartz.NET on ravenDB](https://github.com/iftahbe/quartznet-RavenDB) is a new provider written for Quartz.NET which lets us use the NoSQL [RavenDB](http://ravendb.net/) database as the persistent Job Store for storing and retrieving scheduling data (instead of the old SQL solutions that are built-in Quartz.NET).

###### Installation
First add scheduling to your app using Quart.NET ([example](http://www.quartz-scheduler.net/documentation/quartz-2.x/quick-start.html)).
Then add [Quartz.NET on ravenDB using NuGET](https://www.nuget.org/packages/Quartz.Impl.RavenDB/).

###### Manual configuration
In your code, where you would have [normally configured](http://www.quartz-scheduler.net/documentation/quartz-2.x/tutorial/job-stores.html) Quartz to use a persistent job store, 
you need to add the following configuration property: 
```
["quartz.jobStore.type"] = "Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB"
```
Also, the following configuration **must** be added to app.config or web.config (change the connection string according to your server url and database name).
```
  <connectionStrings>
    <add name="quartznet-ravendb" connectionString="Url=http://localhost:8080;DefaultDatabase=MyDatabaseName"/>
  </connectionStrings>
```

## For example:
```
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
};

// Init scheduler with the desired configuration properties
ISchedulerFactory sf = new StdSchedulerFactory(properties);
<<<<<<< HEAD
IScheduler scheduler = sf.GetScheduler();
```