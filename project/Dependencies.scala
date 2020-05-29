import sbt.{ExclusionRule, _}

object Dependencies {
  object Versions {
    val akka = "2.6.5"
    val akkaHttp = "10.1.11"
    val alpakka = "1.1.2"
    val curatorTestVersion = "2.13.0"
    val curatorVersion = "4.0.1"
    val dropwizard = "4.0.5"
    val mesos = "1.9.0"
    val scalaLogging = "3.9.2"
    val scalaTest = "3.0.8"
  }

  val mesos = "org.apache.mesos" % "mesos" % Versions.mesos
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging

  // transitive
  val scalaAsync = "org.scala-lang.modules" %% "scala-async" % "0.10.0"

  // Akka and akka-http
  val akkaHttp = "com.typesafe.akka" %% "akka-http" % Versions.akkaHttp
  val akkaActor = "com.typesafe.akka" %% "akka-actor" % Versions.akka
  val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.akka
  val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka

  // Apache Curator. See this [compatibility documentation](http://curator.apache.org/zk-compatibility.html)
  private val excludeZk35 = ExclusionRule(organization = "org.apache.zookeeper", name = "zookeeper")

  val curatorRecipes = ("org.apache.curator" % "curator-recipes" % Versions.curatorVersion).excludeAll(excludeZk35)
  val curatorClient = "org.apache.curator" % "curator-client" % Versions.curatorVersion
  val curatorFramework = "org.apache.curator" % "curator-framework" % Versions.curatorVersion
  val curatorAsync = "org.apache.curator" % "curator-x-async" % Versions.curatorVersion
  val curatorTest = ("org.apache.curator" % "curator-test" % Versions.curatorTestVersion).excludeAll(excludeZk35)

  val jwtPlayJson = "com.pauldijou" %% "jwt-play-json" % "4.2.0"
  val akkaHttpPlayJson = "de.heikoseeberger" %% "akka-http-play-json" % "1.31.0"
  val alpakkaCodes = "com.lightbend.akka" %% "akka-stream-alpakka-simple-codecs" % Versions.alpakka

  val rollingMetrics = "com.github.vladimir-bukhtoyarov" % "rolling-metrics" % "2.0.4"
  val scalaJava8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
  val dropwizardMetricsCore = "io.dropwizard.metrics" % "metrics-core" % Versions.dropwizard
  val dropwizardMetricsJvm = "io.dropwizard.metrics" % "metrics-jvm" % Versions.dropwizard
  val hdrHistogram = "org.hdrhistogram" % "HdrHistogram" % "2.1.11"
  val scopt = "com.github.scopt" %% "scopt" % "4.0.0-RC2"

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest

    // testRuntimeOnly "org.scala-lang.modules:scala-xml_$scalaVersion:1.1.1"

    // Logging
    val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3"
    val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka

    // Provides import org.junit.runner.RunWith. We probably want to move to a different test platform for Scalatest or
    // use the Scalatest Runner.
    val junit = "junit" % "junit" % "4.12"
    val junitJupiter = "org.junit.jupiter" % "junit-jupiter-api" % "5.3.1"

    val commonsIO = "commons-io" % "commons-io" % "2.6"
    val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka
  }
}
