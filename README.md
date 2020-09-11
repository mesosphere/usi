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

## Building using Docker sbt image

Assuming you have docker installed, the Makefile targets can be used to clean, build and experiment with the sbt command line.
The docker container runs with the same UID and GID as the calling user preventing permission issues on mounted files.
The sbt working files are mounted from the 'builder' folder, so they persist across invocations.

Make parameters:

    debug=[0|1]: Optional. defaults to 0, set to 1 for debug output from makefile and sbt command
    open_jdk_version=[jdk8u265-b01-centos]: Optional, use this to override default. Image ${open_jdk_image}:${open_jdk_version} must exist.
    open_jdk_image=[adoptopenjdk/openjdk8]: Optional, use this to override default. Image ${open_jdk_image}:${open_jdk_version} must exist.
    mesos_version=[1.9.0]: Optional, use this to override default. Package mesos-${mesos_version} must exist. 
    sbt_version=[1.3.3]: Optional, use this to override default. Package sbt-${sbt_version} must exist.

List available Makefile targets. The 'list' target is optional. Private _targets are not listed.

    make [list]

Build a local docker image called mesos/sbt:${open_jdk_version}-${sbt_version}:

    make [debug=0] [open_jdk_image=adoptopenjdk/openjdk8] [open_jdk_version=jdk8u265-b01-centos] [sbt_version=1.3.3] [mesos_version=1.9.0] docker-build
    
Run the sbt clean command:
 
    make [debug=0] [open_jdk_version=jdk8u265-b01-centos] [sbt_version=1.3.3] sbt-clean

Run the sbt compile command:

    make [debug=0] [open_jdk_version=jdk8u265-b01-centos] [sbt_version=1.3.3] sbt-compile

Run the sbt test command:
FIXME: Some Mesos tests timeout after 30sec. Investigate why.

    make [debug=0] [open_jdk_version=jdk8u265-b01-centos] [sbt_version=1.3.3] sbt-test

Run the sbt package command:

    make [debug=0] [open_jdk_version=jdk8u265-b01-centos] [sbt_version=1.3.3] sbt-package

Run the sbt shell command to experiment with other sbt commands:

    make [debug=0] sbt-shell

Open a docker bash prompt to experiment with the build process.
NOTE: Uses the docker 'root' user, which can create subsequent file permission 
      conflicts in the mounted folders. 
      Use 'deep-clean' target to remove ALL intermediary files

    make [debug=0] docker-shell

Remove the target directories and the sbt working directories.
NOTE: This can be useful if using the 'docker-shell', which can create root permission conflicts

    make [debug=0] deep-clean

