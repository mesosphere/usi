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

```scala
groups.map { group: Group =>
  group.apps
    .groupBy(_.container.image)
    .map { case (image: String, apps: Seq[App]) =>
      image -> apps.length
    }
  }
```

Over:

```scala
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

```scala
import org.company.{
  Stringifier,
  Demuxer
}
```

Over:

```scala
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

```bash
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

All relevant ids should be part of the logging message e.g.:

* pod ID ("podId")
* mesos task ID ("taskId")
* mesos offer ID ("offerId")

It's important to keep the naming of keys consistent so that one can use them for log pre-selection.

Even though structured logging is strongly recommended to every framework developer, including context into the final log message is always optional. We should aim for keeping our messages meaningful even without the context provided (it's ok to duplicate e.g. podId both in messaage and context).

Example of good framework log messages (a little bit simplified) can look like example below:

```json
{
  "time": "1.1.1970",
  "level": "INFO",
  "podId": "my-pod",
  "taskId": "mesos-task-my-pod-1",
  "message": "Task $taskId of Pod $podId was killed"
}
```

## Testing

Tests are executable documentation and should be **written, read and maintained** as such. A test tells its reader a short story about how some part of the system should behave given a certain input. Hundreds of excellent books and articles are dedicated to writing proper tests, so we'll try to avoid repeating them here.

### On Libraries and Styles

We heavily utilize [ScalaTest](http://www.scalatest.org/) as our primary test library. All tests should implement the UnitTest class in test-utils, which uses [WordSpec](http://www.scalatest.org/at_a_glance/WordSpec).

```scala
  // Describe a scope for a subject, in this case: "A Set"
  "A Set" can { // All tests within these curly braces are about "A Set"

    // Can describe nested scopes that "narrow" its outer scopes
    "empty" should { // All tests within these curly braces are about "A Set (when empty)"

      "have size 0" in {    // Here, 'it' refers to "A Set (when empty)". The full name
        Set.empty.size shouldBe 0 // of this test is: "A Set (when empty) should have size 0"
      }
      ...
```

combined with [Given, When, And Then](http://www.scalatest.org/getting_started_with_feature_spec) and scala [matchers](http://www.scalatest.org/user_guide/using_matchers):

```scala
  "A mutable Set" should "allow an element to be added" in {
    Given("an empty mutable Set")
    val set = mutable.Set.empty[String]

    When("an element is added")
    set += "clarity"

    Then("the Set should have size 1")
    set.size shouldBe 1

    And("the Set should contain the added element")
    set should contain theSameElementsAs Set("clarity")
  }
```

which give us an opportunity to write a test as a short story documenting part of the system behaviour.

### On Testing Granularity

In USI we define following granularity levels:

* **Unit tests**: the lowest test level possible. Unit test are cheap, can be run fast and give the developer an immediate feedback on when something is obviously broken
* **Integration tests**: an integration test starts a minimal Mesos cluster consisting of Mesos master, Mesos agent, in-memory Zookeeper server and a framework. These are more expensive tests that typically require more resources and time to run. Tests that require back and forth communication with Mesos are best placed here because we don't want to mock Mesos responses.
* **System integraiton test**: here we start a full-blown DC/OS cluster testing all aspects of framework interacting with the DC/OS ecosystem. These are the most expensive tests (in terms of time and money) that would typically reqire a DC/OS cluster running on e.g. AWS utilizing multiple EC2 nodes, volumes, ELB etc. A system integration test would typically cover some coarse-grained USI feature or a feature that relies on other DC/OS components like the secret store.

**As a rule of thumb, a broken behavior should fail at the lowest possible level**. Most features will be covered on more than one level but the coverage is different. Let's consider support for [Mesos fetcher](http://mesos.apache.org/documentation/latest/fetcher/) as an example. In preparation to run a task, the Mesos fetcher downloads resources into the task's sandbox directory. So how do the tests look like?

* **Unit test**: makes sure that given a `PodSpec` with a defined fetch URI it is converted to a Mesos protobuf message, where correspoding [URI](https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto#L658) fields are initialized properly
* **Integration test**: makes sure that when a task from a `PodSpec` is started, the fetched artifact is actually part of its sandbox
* **System integration test**: this is a tricky one; do we need full DC/OS cluster to test this? How is this different from the integration test from the fetchers perspective? The simple answer is that it's not, and an integration test might be sufficient enough.

However consider the following aspects (which are all taken from prior experience building Mesos frameworks):

* While an integration test usually starts some stable Mesos version on which USI officially depends, DC/OS frequently integrates the latest Mesos changes into its master branch. Testing a framework against the latest DC/OS master might expose a bug sooner rather than later.
* An integration test runs against a local Mesos cluster where communication is typically fast and reliable. A system integration test runs against a cluster somewhere in the cloud, and has a different communications profile. In the past, we've seen bugs that would only manifest themselves in the latter case, but not in the former. Admittedly, it seems unlikely that such a bug is triggered in the Mesos fetcher; however, we've seen unlikely things happen before.
* Even a simple request to the framework running on a DC/OS cluster touches many components on its way (e.g. ELB, Admin Router, Monitoring service which themselves rely on other components, such as Admin Router relying on CockroachDB), so there is always potential for things to go wrong.

It makes sense to have a system integration test for every user-facing coarse-grained API feature, including the example above.
