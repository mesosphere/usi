# Code Culture

## Overview

The code culture is a set of defaults ascribed to by the Orchestration developer team. Since writing software is an optimization problem, we guide our design decisions with defaults, not rules. Deviations from our defaults require justification.

Our defaults are:

* Code is legible on GitHub
* Immutability over mutability
* No smelly code
* Fail loud and proud
* Let it crash
* Don't panic
* Structured logging

## Code is Legible on GitHub

This default speaks to the desire to write code that can be understood without needing an IDE. USI is written in Scala, so our defaults are specific as such.

### On Implicits

Implicits are an important feature in Scala and are critical for the implementation of type-classes and DSLs.

Prefer:

* Explicit conversion over implicit
* Explicit passing over implicit


For our case, if implicits don't help improve the design of code, we don't use them. We avoid implicit conversions. We also avoid multi-priority implicits. When type classes help improve the design of code, we use implicits for type classes (IE serialization logic).

We prefer implicits to be used for contextual values such as execution contexts or the actor system. However, we prefer to be conservative, and pass the value explicitly for things like validators, authenticators, and normalizers.

### On Type Inference

If you are writing some function chain that has multiple levels of inferred function parameter types, we prefer to specify the parameter types.

Prefer:

```
groups.map { group: Group =>
  group.apps
    .groupBy(_.container.image)
    .map { case (image: String, apps: Seq[App]) =>
      image -> apps.length
    }
  }
```

Over:

```
groups.map { group =>
  group.apps.groupBy(_.container.image).map { case(k, v) =>
    k -> v.length
  }
}
```

### On Playing Code Golf

Code golf is a game in which you implement some program in as few characters as possible.

We prefer not to play this game when working on USI. Instead, we focus on removing noise (boilerplate), while preserving valuable signal pertaining to the problem at hand.

### On Imports

We prefer, almost always, that imports go at the top of the file. Additionally, prefer explicit imports over wildcard imports.

Prefer:

```
import org.company.{
  Stringifier,
  Demuxer
}
```

Over:

```
import org.company._
```

## Immutability Over Mutability

Functional programming is what follows when you don't mutate state. We prefer it, except in performance critical parts of our code.

We have a strong preference to encapsulate mutable state. References to data crossing the boundary of some module should be immutable.

## No Smelly Code

We pay attention to [code smells](https://en.wikipedia.org/wiki/Code_smell) and strive to write well encapsulated code.

* A method should not receive more data than it needs to do its job.
* Changing some behavior should not result in "shotgun surgery"

### On TODOs

TODOs are as liberally used as they are ignored. TODOs should be only used for one of two reasons:

1. Things you plan to do before merging
2. Things that are definitely planned in the near future (with a high-priority JIRA)

Instead of a TODO, prefer:

* Code that fails (e.g. throw a not-implemented exception)
* A comment describing the pitfalls of the implementation (e.g. this code does not perform very well, or, this code is prone to bugs from possible rounding errors)

## Fail Loud and Proud

It is better to do nothing, than to do the wrong thing.

One of my favorite non-examples of this is found in PHP 5:

```
$ echo '<? print(strftime("%Y-%m-%d",strtotime("lol"))) ?>' | php
1970-01-01
```

Another non-example is found in Marathon's history. At one point in time, the storage layer would swallow the exception and return `None` if it could not deserialize some entity successfully, and this led to data loss.

We are strict in what we accept, and in what we emit. If input is not what we expect, don't be fancy.

Fail loudly. Fail proudly.

## Let it Crash

We prefer to focus on state recovery, and not graceful tear down. We'd prefer to just crash and restart, rather than implement many complex error recovery handlers.

## Don't Panic

We prefer to that Exceptions are thrown only when something goes unexpectedly wrong outside of domain of the library.

For example:

* A validation function should return validation errors, not throw them.
* A storage module will throw an exception if a connection is unavailable.

## Structured logging

Debugging distributed system is hard. To make this at least a bit easier we favor log messages including logging context (using structured logging). Core USI is providing all necessary tooling for framework developer to log in structured way.

For introduction to structured logging read [this article](https://stackify.com/what-is-structured-logging-and-why-developers-need-it/).

What should always be part of the logging message:
- pod ID ("podId")
- mesos task ID ("taskId")
- mesos offer ID ("offerId")

It's important to keep the naming of keys consistent so that one can use them for log pre-selection.

Even though structured logging is strongly recommended to every framework developer, including context into the final log message is always optional. We should aim for keeping our messages meaningful even without the context provided (it's ok to duplicate e.g. podId both in messaage and context).

Example of good framework log messages (a little bit simplified) can look like example below:

```json
{
  "time": "1.1.1970",
  "level": "INFO",
  "podId": "my-pod",
  "taskId": "mesos-task-my-pod-1",
  "message": "Pod 'my-pod' was killed"
}
```