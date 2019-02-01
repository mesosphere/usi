package com.mesosphere.utils

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import org.scalactic.source.Position
import org.scalatest.concurrent.{Eventually, JavaFutures, ScalaFutures, TimeLimitedTests}
import org.scalatest.time.{Minute, Seconds, Span}
import org.scalatest.{
  AppendedClues,
  Args,
  BeforeAndAfter,
  BeforeAndAfterAll,
  BeforeAndAfterEach,
  GivenWhenThen,
  Informer,
  Matchers,
  OptionValues,
  Status,
  TryValues,
  WordSpec,
  WordSpecLike
}

import scala.concurrent.ExecutionContextExecutor

/**
  * Base trait for all unit tests in WordSpec style with common matching/before/after and Option/Try/Future
  * helpers all mixed in.
  */
trait UnitTestLike
    extends WordSpecLike
    with GivenWhenThen
    with ScalaFutures
    with JavaFutures
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterEach
    with OptionValues
    with TryValues
    with AppendedClues
    with StrictLogging
    with BeforeAndAfterAll
    with Eventually
    with TimeLimitedTests {

  private class LoggingInformer(info: Informer) extends Informer {
    def apply(message: String, payload: Option[Any] = None)(implicit pos: Position): Unit = {
      logger.info(s"===== ${message} (${pos.fileName}:${pos.lineNumber}) =====")
      info.apply(message, payload)(pos)
    }
  }

  /**
    * We wrap the informer so that we can see where we are in the test in the logs
    */
  override protected def info: Informer = {
    new LoggingInformer(super.info)
  }

  abstract protected override def runTest(testName: String, args: Args): Status = {
    logger.info(s"=== Test: ${testName} ===")
    super.runTest(testName, args)
  }

  override val timeLimit = Span(1, Minute)

  override implicit lazy val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds))
}

abstract class UnitTest extends WordSpec with UnitTestLike

trait AkkaUnitTestLike extends UnitTestLike {
  protected lazy val akkaConfig: Config = ConfigFactory.parseString(s"""
       |akka.test.default-timeout=${patienceConfig.timeout.millisPart}
    """.stripMargin).withFallback(ConfigFactory.load())

  implicit lazy val system: ActorSystem = ActorSystem(suiteName, akkaConfig)
  implicit lazy val scheduler: Scheduler = system.scheduler
  implicit lazy val mat: Materializer = ActorMaterializer(ActorMaterializerSettings(system))
  implicit lazy val ctx: ExecutionContextExecutor = system.dispatcher

  abstract override def afterAll(): Unit = {
    super.afterAll()
    system.terminate().futureValue // intentionally shutdown the actor system last.
  }
}

abstract class AkkaUnitTest extends UnitTest with AkkaUnitTestLike
