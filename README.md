
Hangfire.PostgreSql
===================
[![Build status](https://ci.appveyor.com/api/projects/status/n05446uxa1f5sjw3?svg=true)](https://ci.appveyor.com/project/ahydrax/hangfire-postgresql)
[![Code quality](https://sonarcloud.io/api/project_badges/measure?project=Hangfire.PostgreSql.ahydrax&metric=alert_status)](https://sonarcloud.io/dashboard?id=Hangfire.PostgreSql.ahydrax)
[![NuGet](https://img.shields.io/nuget/v/Hangfire.PostgreSql.ahydrax.svg)](https://www.nuget.org/packages/Hangfire.PostgreSql.ahydrax/)
[![GitHub license](https://img.shields.io/badge/license-LGPL-blue.svg?style=flat)](https://raw.githubusercontent.com/ahydrax/Hangfire.PostgreSql/master/COPYING)

This is a plugin for Hangfire to enable PostgreSQL as a storage system.
Read about hangfire here: https://github.com/HangfireIO/Hangfire#hangfire-
and here: http://hangfire.io/

Requirements
------------
* .NET Framework: `.>=NET 4.5.2` or `>=.NET Standard 1.6`
* PostgreSql: `>=9.6`

Instructions
------------
Install Hangfire, see https://github.com/HangfireIO/Hangfire#installation

Download source files and build your own binaries or just use nuget package.

```csharp
app.UseHangfireServer(new BackgroundJobServerOptions(), 
  new PostgreSqlStorage("<connection string>"));
app.UseHangfireDashboard();
```

Additional metrics for Hangfire.Dashboard
-----------------
There are 6 different metrics you can use:

![dashboard](content/dashboard.png)

```csharp
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.MaxConnections);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.ActiveConnections);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.DistributedLocksCount);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlLocksCount);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.CacheHitsPerRead);
GlobalConfiguration.Configuration.UseDashboardMetric(PostgreSqlDashboardMetrics.PostgreSqlServerVersion);
```

Backward compatibility with original project
-----------------
* Minimum required PostgreSQL version is 9.6
* `PostgreSqlStorageOptions.SchemaName` is removed. Consider using `SearchPath` in your connection string.
* Connection string must be passed directly to constructor or bootstrapper method (it is no longer available to pass connection string name stored in ```app.config```);
* Constructor with existing `NpgsqlConnection` is no longer available;
* Removed parameter UseNativeDatabaseTransactions (transactions are used where needed and it can't be turned off);
* Anything else I've already forgotten.


Related links
-----------------

* [Hangfire.Core](https://github.com/HangfireIO/Hangfire)
* [Hangfire.Postgres original project](https://github.com/frankhommers/Hangfire.PostgreSql)

License
========

Copyright © 2014-2017 Frank Hommers, Burhan Irmikci (barhun), Zachary Sims(zsims), kgamecarter, Stafford Williams (staff0rd), briangweber, Viktor Svyatokha (ahydrax), Christopher Dresel (Dresel), Vytautas Kasparavičius (vytautask).

Hangfire.PostgreSql is an Open Source project licensed under the terms of the LGPLv3 license. Please see http://www.gnu.org/licenses/lgpl-3.0.html for license text or COPYING.LESSER file distributed with the source code.

This work is based on the work of Sergey Odinokov, author of Hangfire. <http://hangfire.io/>
