# Metrics

This is a [Dropwizard Metrics](https://github.com/dropwizard/metrics) implementation 
of the USI metrics interface. It supports multiple exporters ([DataDog(https://www.datadoghq.com/)],
[StatsD](https://github.com/etsy/statsd)) as well as text representation in 
[Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/) format.

Histograms and timers are backed with reservoirs leveraging
[HdrHistogram](http://hdrhistogram.org/).

## Metric names

All metric names are prefixed with `usi` by default. This can be changed
using `usi.metrics.name-prefix` configuration parameter. Metric name components 
are joined with dots. Components may have dashes in them.

A metric type and a unit of measurement (if any) are appended to
a metric name. A couple of examples:

* `usi.scheduler.offers.counter`
* `usi.scheduler.offers.processing-time.seconds`

## Prometheus reporter

The Prometheus reporter can be used to return a text representation of 
current metrics and their values. Dots and dashes in metric names are 
replaced with underscores.

## StatsD reporter

The StatsD reporter can be enabled with `usi.metrics.statsd-reporter` parameter. 
It sends metrics over UDP to the host and port specified with
`usi.metrics.reporters.statsd.host` and `usi.metrics.reporters.statsd.port` respectively.

Counters are forwarded as gauges.

## DataDog reporter

The DataDog reporter can be enabled with `usi.metrics.datadog-reporter` parameter. 
It sends metrics over UDP to the host and port specified with `usi.metrics.reporters.datadog.host` 
and `usi.metrics.reporters.datadog.port` respectively.

Metrics can be sent to a DataDog agent over UDP, or directly to
the DataDog cloud over HTTP. It is specified using
`usi.metrics.reporters.datadog.protocol`. Its possible values are `udp` (default)
and `api`. If `api` is chosen, your DataDog API key can be supplied with
`usi.metrics.reporters.datadog.api-key`.

Dashes in metric names are replaced with underscores. Counters are
forwarded as gauges.


### JVM-specific metrics
A selected set of JVM related metrics is collected by default.

#### JVM buffer pools

* `usi.jvm.buffers.mapped.gauge` — an estimate of the number of
  mapped buffers.
* `usi.jvm.buffers.mapped.capacity.gauge.bytes` — an estimate of
  the total capacity of the mapped buffers in bytes.
* `usi.jvm.buffers.mapped.memory.used.gauge.bytes` an estimate of
  the memory that the JVM is using for mapped buffers in bytes, or `-1L`
  if an estimate of the memory usage is not available.
* `usi.jvm.buffers.direct.gauge` — an estimate of the number of
  direct buffers.
* `usi.jvm.buffers.direct.capacity.gauge.bytes` — an estimate of
  the total capacity of the direct buffers in bytes.
* `usi.jvm.buffers.direct.memory.used.gauge.bytes` an estimate of
  the memory that the JVM is using for direct buffers in bytes, or `-1L`
  if an estimate of the memory usage is not available.

#### JVM garbage collection

* `usi.jvm.gc.<gc>.collections.gauge` — the total number
  of collections that have occurred
* `usi.jvm.gc.<gc>.collections.duraration.gauge.seconds` — the
  approximate accumulated collection elapsed time, or `-1` if the
  collection elapsed time is undefined for the given collector.

#### JVM memory

* `usi.jvm.memory.total.init.gauge.bytes` - the amount of memory
  in bytes that the JVM initially requests from the operating system
  for memory management, or `-1` if the initial memory size is
  undefined.
* `usi.jvm.memory.total.used.gauge.bytes` - the amount of used
  memory in bytes.
* `usi.jvm.memory.total.max.gauge.bytes` - the maximum amount of
  memory in bytes that can be used for memory management, `-1` if the
  maximum memory size is undefined.
* `usi.jvm.memory.total.committed.gauge.bytes` - the amount of
  memory in bytes that is committed for the JVM to use.
* `usi.jvm.memory.heap.init.gauge.bytes` - the amount of heap
  memory in bytes that the JVM initially requests from the operating
  system for memory management, or `-1` if the initial memory size is
  undefined.
* `usi.jvm.memory.heap.used.gauge.bytes` - the amount of used heap
  memory in bytes.
* `usi.jvm.memory.heap.max.gauge.bytes` - the maximum amount of
  heap memory in bytes that can be used for memory management, `-1` if
  the maximum memory size is undefined.
* `usi.jvm.memory.heap.committed.gauge.bytes` - the amount of heap
  memory in bytes that is committed for the JVM to use.
* `usi.jvm.memory.heap.usage.gauge` - the ratio of
  `usi.jvm.memory.heap.used.gauge.bytes` and
  `usi.jvm.memory.heap.max.gauge.bytes`.
* `usi.jvm.memory.non-heap.init.gauge.bytes` - the amount of
  non-heap memory in bytes that the JVM initially requests from the
  operating system for memory management, or `-1` if the initial memory
  size is undefined.
* `usi.jvm.memory.non-heap.used.gauge.bytes` - the amount of used
  non-heap memory in bytes.
* `usi.jvm.memory.non-heap.max.gauge.bytes` - the maximum amount of
  non-heap memory in bytes that can be used for memory management, `-1`
  if the maximum memory size is undefined.
* `usi.jvm.memory.non-heap.committed.gauge.bytes` - the amount of
  non-heap memory in bytes that is committed for the JVM to use.
* `usi.jvm.memory.non-heap.usage.gauge` - the ratio of
  `usi.jvm.memory.non-heap.used.gauge.bytes` and
  `usi.jvm.memory.non-heap.max.gauge.bytes`.

#### JVM threads

* `usi.jvm.threads.active.gauge` — the number of active threads.
* `usi.jvm.threads.daemon.gauge` — the number of daemon threads.
* `usi.jvm.threads.deadlocked.gauge` — the number of deadlocked
  threads.
* `usi.jvm.threads.new.gauge` — the number of threads in `NEW`
   state.
* `usi.jvm.threads.runnable.gauge` — the number of threads in
  `RUNNABLE` state.
* `usi.jvm.threads.blocked.gauge` — the number of threads in
  `BLOCKED` state.
* `usi.jvm.threads.timed-waiting.gauge` — the number of threads in
  `TIMED_WAITING` state.
* `usi.jvm.threads.waiting.gauge` — the number of threads in
  `WAITING` state.
* `usi.jvm.threads.terminated.gauge` — the number of threads in
  `TERMINATED` state.
