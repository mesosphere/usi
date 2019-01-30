# Unified Scheduler Interface
Deploy and manage containers (including Docker) on top of Apache Mesos at scale.

This repository is currently a work-in-progress.

[Design principles behind USI](https://github.com/mesosphere/marathon-design/blob/master/unified-scheduler/index.md)

How to run:

```
$ gradle :hello-world:run
```


How to test:

```
$ gradle test
```

## Versioning

USI uses [semantic versioning](https://semver.org/). That means 
we are committed to keep our documented API compatible across releases 
unless we change the MAJOR version (the first number in the version tuple).

For example version 1.1.0 and 2.0.0 will not have a compatible API, but 
1.2.0 and 1.1.0 should be backwards compatible. 

Please take note that API below version 1.0.0 aren't considered stable and may
change at any time.