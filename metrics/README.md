# Metrics

This module defines a set of metrics interfaces. For a concrete implementation 
see [metrics-dropwizar](http://github.com/mesosphere/use/metrics-dropwizard) 
module. 

## Metric types

USI has the following metric types:

* a `counter` is a monotonically increasing integer, for instance, the
  number of Mesos `revive` calls performed since the framework became
  a leader.
* a `gauge` is a current measurement, for instance, the number of tasks
  currently known to the framework.
* a `histogram` is a distribution of values in a stream of measurements,
  for instance, the number of apps in group deployments.
* a `meter` measures the rate at which a set of events occur.
* a `timer` is a combination of a meter and a histogram, which measure
  the duration of events and the rate of their occurrence.

## Units of measurement

A metric measures something either in abstract quantities, or in the
following units:

* `bytes`
* `seconds`
