# Getting Started

## Project Setup 

Given you have added the `https://downloads.mesosphere.com/maven` Maven repository you can declare the following dependencies

@@dependency[sbt,Maven,Gradle] {
  group="com.mesosphere.usi" artifact="core_$scala.binary.version$" version="$project.version$"
  group2="com.mesosphere.usi" artifact2="core-model_$scala.binary.version$" version2="$project.version$"
  group3="com.mesosphere.usi" artifact3="mesos-client_$scala.binary.version$" version3="$project.version$"
  group4="com.mesosphere.usi" artifact4="metrics-dropwizard_$scala.binary.version$" version4="$project.version$"
}

## Hello World

Let's start out with a simple `Hello World` program. We are going to connect to Mesos (1), start a task that echos `Hello World` (2) and then finish (3).

### 1 Connect to Mesos

@@snip [CoreHelloWorlFramework.scala](/examples/core-hello-world/src/main/scala/com/mesosphere/usi/examples/CoreHelloWorldFramework.scala) { #connect }

The `MesosClient(...)` factory method returns a future `MesosClient`. It requires the Mesos [FrameworkInfo](http://mesos.apache.org/api/latest/java/org/apache/mesos/Protos.FrameworkInfo.html). In this case we block until Mesos is connected. You should not do so in the real world. Also note, that we are not retrying the connection. See the section on [Resilience](#).

After the connection is established we create a USI scheduler flow. This is an [Akka Stream](https://doc.akka.io/docs/akka/current/stream/index.html) flow that receives @scaladoc[SchedulerCommand](com.mesosphere.usi.core.models.commands.SchedulerCommand)s and outputs @scaladoc[StateEvent](com.mesosphere.usi.core.models.StateEvent)s. 

### 2 Start a Task

Once the client is connected and scheduler flow created we run a source via the flow.

@@snip [CoreHelloWorlFramework.scala](/examples/core-hello-world/src/main/scala/com/mesosphere/usi/examples/CoreHelloWorldFramework.scala) { #command-source }

The source is just a single @scaladoc[LaunchPod](com.mesosphere.usi.core.models.commands.LaunchPod) command:

@@snip [CoreHelloWorlFramework.scala](/examples/core-hello-world/src/main/scala/com/mesosphere/usi/examples/CoreHelloWorldFramework.scala) { #launch-command }

We do not have to handle offer matching since USI is taking this work from our shoulder. We just care about what we want to launch.

### 3 Finish

The last step handles the events emitted by USI.

@@snip [CoreHelloWorlFramework.scala](/examples/core-hello-world/src/main/scala/com/mesosphere/usi/examples/CoreHelloWorldFramework.scala) { #event-handling }

In this simple case we just inform about the state of our task and finish. USI has launch at most once guarantees. That means it is upt to you the framework author to re-launch finished and failed tasks.

