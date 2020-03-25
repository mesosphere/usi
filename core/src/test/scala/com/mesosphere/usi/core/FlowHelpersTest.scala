package com.mesosphere.usi.core

import java.util.UUID

import akka.stream.KillSwitches
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.mesosphere.utils.AkkaUnitTest

import scala.util.{Failure, Success}

class FlowHelpersTest extends AkkaUnitTest {
  val inputElement = "one"
  val outputElement = 1

  def newMockedFlow[M, O](
      inputSink: Sink[String, M],
      outputSource: Source[Int, O]): (Flow[String, Int, NotUsed], M, O) = {

    val (m, preMaterializedInput) =
      Flow[String]
        .toMat(inputSink)(Keep.right)
        .preMaterialize()

    val (o, preMaterializedOutput) =
      outputSource.preMaterialize()

    val flow = Flow.fromSinkAndSourceCoupled(preMaterializedInput, preMaterializedOutput)

    (flow, m, o)
  }

  "materialized as Source and Sink" should {
    "Push the input elements downstream" in {
      val (flow, firstElement, _) =
        newMockedFlow(inputSink = Sink.head, outputSource = Source.maybe)
      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      source.runWith(Sink.ignore)
      Source.repeat(inputElement).runWith(sink)

      firstElement.futureValue shouldEqual inputElement
    }

    "Push the events from flow to the provided Source" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.repeat(outputElement))
      val (source, _) = FlowHelpers.asSourceAndSink(flow)

      source.runWith(Sink.head).futureValue shouldEqual outputElement
    }

    "Complete both sink and source when the event stream coming to flow is cancelled by the flow" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.head, outputSource = Source.repeat(outputElement))
      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val inputCompleted = Source
        .repeat(inputElement)
        .watchTermination()(Keep.right)
        .to(sink)
        .run()

      val outputCompleted = source
        .watchTermination()(Keep.right)
        .to(Sink.ignore)
        .run()

      inputCompleted.futureValue shouldBe Done
      outputCompleted.futureValue shouldBe Done
    }

    "Complete both sink and source when the event stream coming to flow is cancelled by the client" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.repeat(outputElement))
      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val inputCompleted = Source
        .single(inputElement) // issue single input element and then cancel the stream
        .watchTermination()(Keep.right)
        .to(sink)
        .run()

      val outputStream = source.runWith(Sink.ignore)

      inputCompleted.futureValue shouldBe Done
      outputStream.futureValue shouldBe Done
    }

    "Complete both sink and source when the event stream outgoing from flow is cancelled by the flow" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.single(outputElement))

      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val inputCompleted = Source
        .repeat(inputElement)
        .watchTermination()(Keep.right)
        .to(sink)
        .run()

      val outputCompleted = source
        .runWith(Sink.ignore)

      inputCompleted.futureValue shouldBe Done
      outputCompleted.futureValue shouldBe Done
    }

    "Complete both sink and source when the event stream outgoing from flow is cancelled by the client" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.repeat(outputElement))

      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val inputCompleted =
        Source
          .repeat(inputElement)
          .runWith(sink)

      val outputCompleted =
        source
          .watchTermination()(Keep.right)
          .to(Sink.head)
          .run()

      inputCompleted.futureValue shouldEqual Done
      outputCompleted.futureValue shouldEqual Done
    }

    "Fail both sink and source when the failure occurred in the upstream" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.repeat(outputElement))

      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val ex = new RuntimeException("Boom!")

      Source
        .failed(ex)
        .prepend(Source.single(inputElement))
        .runWith(sink)

      val outputCompleted =
        source
          .watchTermination()(Keep.right)
          .runWith(Sink.ignore)

      outputCompleted.failed.futureValue shouldEqual ex
    }

    "Cancels the input sink when the failure occurred in the downstream" in {
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.repeat(outputElement))
      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val inputCompleted = Source
        .repeat(inputElement)
        .runWith(sink)

      source
        .map(_ => throw new Exception("Boom"))
        .runWith(Sink.ignore)

      inputCompleted.futureValue shouldEqual Done
    }

    "Propagates an exception to the output source if an exception is received from the input" in {
      val (flow, _, _) = newMockedFlow(inputSink = Sink.ignore, outputSource = Source.maybe)
      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val ex = new Exception("boom")
      Source
        .failed(ex)
        .runWith(sink)

      val outputCompleted = source
        .runWith(Sink.ignore)

      outputCompleted.failed.futureValue shouldEqual ex
    }

    "Stop sink and fail source when the failure occurred in the output" in {
      val ex = new Exception("Boom!")
      val (flow, _, _) =
        newMockedFlow(inputSink = Sink.ignore, outputSource = Source.failed(ex))

      val (source, sink) = FlowHelpers.asSourceAndSink(flow)

      val inputCompleted = Source
        .repeat(inputElement)
        .runWith(sink)

      val outputCompleted = source
        .runWith(Sink.ignore)

      inputCompleted.futureValue shouldEqual Done
      outputCompleted.failed.futureValue shouldEqual ex
    }
  }
}
