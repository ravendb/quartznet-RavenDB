# Quartz.NET-RavenDB
JobStore implementation for Quartz.NET scheduler using RavenDB.

###### About
[Quartz.NET](https://github.com/quartznet/quartznet) is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

[Quartz.NET on RavenDB](https://github.com/ravendb/quartznet-RavenDB) is a new provider written for Quartz.NET which lets us use the  [RavenDB](https://ravendb.net/features) NoSQL database as the persistent Job Store for scheduling data (instead of the SQL solutions that are built-in Quartz.NET).

###### Release Notes
* 1.0.5 - Fixed authentication problem when using ApiKey in the connection string	
* 1.0.4 - Fixed misfire handling bug causing jobs not to fire. 
* 1.0.3 - Fixed a bug causing jobs and triggers not to be deleted.
* 1.0.2 - Removed C5 dependency & minor bug fixes.
* 1.0.0 - Initial release.

###### Installation
First add scheduling to your app using Quartz.NET ([example](http://www.quartz-scheduler.net/documentation/quartz-2.x/quick-start.html)).
Then install the NuGet [package](https://www.nuget.org/packages/Quartz.Impl.RavenDB/).

###### Manual configuration
In your code, where you would have [normally configured](http://www.quartz-scheduler.net/documentation/quartz-2.x/tutorial/job-stores.html) Quartz to use a persistent job store, 
you must add the following configuration property: 
```
quartz.jobStore.type = Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB
```
Also, the following configuration must be added to app.config or web.config (change the connection string according to your server url and database name).
```
  <connectionStrings>
    <add name="quartznet-ravendb" connectionString="Url=http://localhost:8080;DefaultDatabase=MyDatabaseName"/>
  </connectionStrings>
```

###### Usage:
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
IScheduler scheduler = sf.GetScheduler();
```

You can also take a look at the following [demo](https://github.com/ravendb/quartznet-RavenDB/blob/master/src/Examples/RavenJobStoreDemo.cs) and you are more than welcome to contribute and make suggestions for improvements.
