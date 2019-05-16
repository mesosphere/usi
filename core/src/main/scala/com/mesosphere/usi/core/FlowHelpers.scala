package com.mesosphere.usi.core

import akka.{Done, NotUsed}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.Future

private[usi] object FlowHelpers {
  def asSourceAndSink[A, B](flow: Flow[A, B, NotUsed])(
      implicit mat: Materializer): (Source[B, NotUsed], Sink[A, Future[Done]]) = {

    val ((inputSubscriber, subscriberCompleted), inputSource) =
      Source.asSubscriber[A].watchTermination()(Keep.both).preMaterialize()

    val source = inputSource.via(flow)

    (source, Sink.fromSubscriber(inputSubscriber).mapMaterializedValue(_ => subscriberCompleted))
  }
}
