# Unified Scheduler Interface

<p align="center">
  Deploy and manage containers (including Docker) on top of Apache Mesos at scale.
</p>
<p align="center">
  <a href="https://travis-ci.org/mesosphere/usi"><img alt="undefined" src="https://img.shields.io/travis/mesosphere/usi/master.svg?style=flat"></a>
  <a href="https://github.com/mesosphere/usi/blob/master/LICENSE"><img alt="undefined" src="https://img.shields.io/github/license/mesosphere/usi.svg?style=flat"></a>
</p>

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
