# Quartz.NET-RavenDB

JobStore implementation for Quartz.NET scheduler using RavenDB.

[![Build status](https://ci.appveyor.com/api/projects/status/y8252pb1yn59yul0?svg=true)](https://ci.appveyor.com/project/nefarius/quartznet-ravendb)

###### About

[Quartz.NET](https://github.com/quartznet/quartznet) is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.

[Quartz.NET on RavenDB](https://github.com/ravendb/quartznet-RavenDB) is a new provider written for Quartz.NET which lets us use the [RavenDB](https://ravendb.net/features) NoSQL database as the persistent Job Store for scheduling data (instead of the SQL solutions that are built-in Quartz.NET).

###### To-Do

- ~~Migrate to Quartz.NET 3.x~~
- ~~Migrate to RavenDB 5.x~~
- ~~Migrate from `Common.Logging` to `Microsoft.Extensions.Logging`~~
  - Removed dependency completely for now
- Overhaul and test configuration system
- ~~Adopt [TAP model](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/async/task-asynchronous-programming-model) everywhere~~
- Migrate Test and Sample projects to SDK format
  - Partially done, test project needs to be adjusted to breaking changes in Quartz.NET upgrade
- Performance improvements
  - Currently there's a lot of small calls made to the DB in various locations that can be optimized with bulk load operations
  - ~~Auto-Indexes get created despite manual index creation, drop these entirely and let the server create necessary indexes?~~
  - Document existence checks are used before loading them, this can be avoided by just attempting to load the document and check the result for null

###### Release Notes

- 1.0.6 - Fixed parsing of Connection String when ApiKey is missing
- 1.0.5 - Fixed authentication problem when using ApiKey in the connection string
- 1.0.4 - Fixed misfire handling bug causing jobs not to fire.
- 1.0.3 - Fixed a bug causing jobs and triggers not to be deleted.
- 1.0.2 - Removed C5 dependency & minor bug fixes.
- 1.0.0 - Initial release.

###### Installation

First add scheduling to your app using Quartz.NET ([example](http://www.quartz-scheduler.net/documentation/quartz-2.x/quick-start.html)).
Then install the NuGet [package](https://www.nuget.org/packages/Quartz.Impl.RavenDB/).

###### Manual configuration

In your code, where you would have [normally configured](http://www.quartz-scheduler.net/documentation/quartz-2.x/tutorial/job-stores.html) Quartz to use a persistent job store, 
you must add the following configuration property: 

```csharp
quartz.jobStore.type = Quartz.Impl.RavenDB.RavenJobStore, Quartz.Impl.RavenDB
```

Also, the following configuration must be added to app.config or web.config (change the connection string according to your server url, database name and ApiKey).

```xml
  <connectionStrings>
    <add name="quartznet-ravendb" connectionString="Url=http://localhost:8080;DefaultDatabase=MyDatabaseName;ApiKey=MyKey/MySecret"/>
  </connectionStrings>
```

###### Usage

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
};

// Init scheduler with the desired configuration properties
ISchedulerFactory sf = new StdSchedulerFactory(properties);
IScheduler scheduler = sf.GetScheduler();
```

You can also take a look at the following [demo](https://github.com/ravendb/quartznet-RavenDB/blob/master/src/Examples/RavenJobStoreDemo.cs) and you are more than welcome to contribute and make suggestions for improvements.
