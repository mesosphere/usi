package com.mesosphere.mesos.client
import java.net.URL

import akka.http.scaladsl.model.Uri
import akka.stream.KillSwitches
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.mesosphere.utils.AkkaUnitTest
import org.apache.mesos.v1.Protos.{FrameworkID, MasterInfo}
import org.apache.mesos.v1.scheduler.Protos.Event

import scala.concurrent.duration._

class MesosClientImplTest extends AkkaUnitTest {

  implicit val askTimeout = Timeout(1.minute)

  "Mesos client" should {
    "fail if the provided Mesos version is incompatible" in {
      val masterInfo = MasterInfo.newBuilder().setVersion("1.8.0").setId("unique").setIp(42).setPort(0).build()
      val frameworkId = FrameworkID.newBuilder().setValue("framework").build()
      val subscribedEvent = Event.Subscribed.newBuilder().setMasterInfo(masterInfo).setFrameworkId(frameworkId).build()
      val sharedKillSwitch = KillSwitches.shared("mesos-client-killswitch")
      val session = Session(Uri("http://localhost"), "stream-id")

      a[IllegalArgumentException] should be thrownBy {
        new MesosClientImpl(null, sharedKillSwitch, subscribedEvent, session, Source.empty)
      }
    }

    "pass if the provided Mesos version is compatible" in {
      val masterInfo = MasterInfo.newBuilder().setVersion("1.9.0").setId("unique").setIp(42).setPort(0).build()
      val frameworkId = FrameworkID.newBuilder().setValue("framework").build()
      val subscribedEvent = Event.Subscribed.newBuilder().setMasterInfo(masterInfo).setFrameworkId(frameworkId).build()
      val sharedKillSwitch = KillSwitches.shared("mesos-client-killswitch")
      val session = new Session(Uri("http://localhost"), "stream-id")

      noException should be thrownBy {
        new MesosClientImpl(null, sharedKillSwitch, subscribedEvent, session, Source.empty)
      }
    }
  }
}
