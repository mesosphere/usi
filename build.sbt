val commonSettings = Seq(
  organization := "com.mesosphere.usi",
  version := {
    import sys.process._
      ("./version" !!).trim
  },
  libraryDependencies := Seq(
    Dependencies.Test.akkaSlf4j % "test",
    Dependencies.Test.logbackClassic % "test"
  ),
  scalaVersion := "2.13.1",
  crossScalaVersions := Seq("2.13.1", "2.12.7"),
)

lazy val `core-models` = (project in file("./core-models/"))
  .settings(commonSettings : _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.mesos,
      Dependencies.scalaLogging,
      Dependencies.Test.scalaTest % "test")) // note, core-models cannot depends on test-utils because this creates a circular dependency.

lazy val `metrics` = (project in file("./metrics"))
  .settings(commonSettings : _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.akkaStream))

lazy val `persistence` = (project in file("./persistence"))
  .settings(commonSettings : _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.akkaActor
    ))
  .dependsOn(`core-models`)

lazy val `test-utils` = (project in file("./test-utils/"))
  .settings(commonSettings : _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Test.scalaTest,
      Dependencies.akkaStream,
      Dependencies.scalaAsync % "compile",
      Dependencies.Test.junit,
      Dependencies.Test.scalaTestJunit,
      Dependencies.akkaHttpPlayJson,
      Dependencies.Test.commonsIO,
      Dependencies.Test.junitJupiter,
      Dependencies.curatorRecipes,
      Dependencies.curatorClient,
      Dependencies.curatorFramework,
      Dependencies.curatorAsync,
      Dependencies.curatorTest,
    ))
  .dependsOn(`core-models` % "compile->compile")
  .dependsOn(`metrics`)
  .dependsOn(`persistence`)

lazy val `mesos-client` = (project in file("./mesos-client/"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.akkaStream,
      Dependencies.akkaHttp,
      Dependencies.akkaHttpPlayJson,
      Dependencies.jwtPlayJson,
      Dependencies.scalaAsync % "compile",
      Dependencies.alpakkaCodes
    ))
  .dependsOn(`core-models`)

lazy val `persistence-zookeeper` = (project in file("./persistence-zookeeper"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.akkaStream,
      Dependencies.curatorFramework,
      Dependencies.curatorAsync,
      Dependencies.Test.akkaStreamTestKit % "test"))
  .dependsOn(`persistence`)
  .dependsOn(`metrics`)
  .dependsOn(`test-utils` % "test->compile")

lazy val `metrics-dropwizard` = (project in file("./metrics-dropwizard"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.akkaHttp,
      Dependencies.scalaLogging,
      Dependencies.rollingMetrics,
      Dependencies.scalaJava8Compat,
      Dependencies.dropwizardMetricsCore,
      Dependencies.dropwizardMetricsJvm,
      Dependencies.hdrHistogram,
    ))
  .dependsOn(`metrics`)
  .dependsOn(`test-utils` % "test->compile")

lazy val `mesos-master-detector` = (project in file("./mesos-master-detector"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.akkaHttpPlayJson,
      Dependencies.scalaAsync % "compile",
      Dependencies.alpakkaCodes))
  .dependsOn(`persistence-zookeeper`)
  .dependsOn(`test-utils` % "test->compile")

lazy val `core` = (project in file("./core"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Test.akkaStreamTestKit % "test"))
  .dependsOn(`core-models`)
  .dependsOn(`persistence`)
  .dependsOn(`mesos-client`)
  .dependsOn(`metrics`)
  .dependsOn(`test-utils` % "test->compile")

lazy val `examples-core-hello-world` = (project in file("./examples/core-hello-world"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Test.akkaStreamTestKit % "test"))
  .dependsOn(`core`)
  .dependsOn(`test-utils` % "compile->compile")

lazy val `examples-keep-alive-framework` = (project in file("./examples/keep-alive-framework"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.Test.akkaStreamTestKit % "test"))
  .dependsOn(`core`)
  .dependsOn(`test-utils` % "compile->compile")

lazy val `examples-mesos-client-example` = (project in file("./examples/mesos-client-example"))
  .settings(commonSettings: _*)
  .dependsOn(`mesos-client`)


lazy val `examples-simple-hello-world` = (project in file("./examples/simple-hello-world"))
  .settings(commonSettings: _*)
  // .settings(
  //   libraryDependencies ++= Seq())
  .dependsOn(`mesos-client`)
  .dependsOn(`test-utils` % "test->compile")
