package com.mesosphere.usi.core
import com.mesosphere.usi.core.models.resources.ResourceType
import com.mesosphere.usi.core.protos.ProtoBuilders
import com.mesosphere.usi.core.protos.ProtoConversions._
import com.mesosphere.utils.UnitTest
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.Resource.DiskInfo.Persistence
import org.apache.mesos.v1.Protos.Resource.{DiskInfo, ReservationInfo}

class ResourceUtilTest extends UnitTest {
  def mountSource(path: Option[String]): DiskInfo.Source = {
    val b = DiskInfo.Source.newBuilder.setType(DiskInfo.Source.Type.MOUNT)
    path.foreach { p =>
      b.setMount(DiskInfo.Source.Mount.newBuilder.setRoot(p))
    }

    b.build
  }

  def mountDisk(path: Option[String]): Protos.Resource.DiskInfo = {
    // val source = Mesos.Resource.DiskInfo.sour
    Protos.Resource.DiskInfo.newBuilder.setSource(mountSource(path)).build
  }

  def reservedForRole(role: String): Protos.Resource.ReservationInfo = {
    ProtoBuilders.newResourceReservationInfo(Protos.Resource.ReservationInfo.Type.STATIC, role, "some-principal")
  }

  def newScalarResource[T: Numeric](
      resourceType: ResourceType,
      amount: T,
      reservations: Iterable[Protos.Resource.ReservationInfo] = Nil,
      disk: Option[DiskInfo] = None,
      role: String = "mock-role"
  ): Protos.Resource = {
    ProtoBuilders.newResource(
      resourceType.name,
      Protos.Value.Type.SCALAR,
      ProtoBuilders.newResourceAllocationInfo(role),
      scalar = amount.asProtoScalar,
      reservations = reservations,
      disk = disk.getOrElse(null)
    )
  }

  "ResourceUtil" should {
    "have no leftOvers for empty resources" in {
      val leftOvers = ResourceUtil.consumeResources(
        Seq(),
        Seq(ports(2 to 12))
      )
      assert(leftOvers == Seq())
    }

    "match on mix of resources" in {
      val leftOvers = ResourceUtil.consumeResources(
        Seq(
          newScalarResource(ResourceType.CPUS, 3),
          ports(2 to 20),
          set(ResourceType.UNKNOWN("labels"), Set("a", "b"))
        ),
        Seq(newScalarResource(ResourceType.CPUS, 2), ports(2 to 12), set(ResourceType.UNKNOWN("labels"), Set("a")))
      )
      assert(
        leftOvers == Seq(
          newScalarResource(ResourceType.CPUS, 1),
          ports(13 to 20),
          set(ResourceType.UNKNOWN("labels"), Set("b"))
        )
      )
    }

    "combine used resource and match on all available resource" in {
      val leftOvers = ResourceUtil.consumeResources(
        Seq(newScalarResource(ResourceType.CPUS, 3)),
        Seq(newScalarResource(ResourceType.CPUS, 2), newScalarResource(ResourceType.CPUS, 1))
      )
      assert(leftOvers == Seq())
    }

    "consider roles when consuming resources" in {
      val leftOvers = ResourceUtil.consumeResources(
        Seq(
          newScalarResource(ResourceType.CPUS, 2),
          newScalarResource(ResourceType.CPUS, 2, reservations = Seq(reservedForRole("marathon")))
        ),
        Seq(
          newScalarResource(ResourceType.CPUS, 0.5),
          newScalarResource(ResourceType.CPUS, 1, reservations = Seq(reservedForRole("marathon"))),
          newScalarResource(ResourceType.CPUS, 0.5, reservations = Seq(reservedForRole("marathon")))
        )
      )
      assert(
        leftOvers == Seq(
          newScalarResource(ResourceType.CPUS, 1.5),
          newScalarResource(ResourceType.CPUS, 0.5, reservations = Seq(reservedForRole("marathon")))
        )
      )
    }

    "consider reservation state when consuming resources" in {
      val reservationInfo = ProtoBuilders.newResourceReservationInfo(
        ReservationInfo.Type.STATIC,
        role = "marathon",
        principal = "principal"
      )

      val disk = DiskInfo.newBuilder().setPersistence(Persistence.newBuilder().setId("persistenceId")).build()
      val resourceWithReservation = newScalarResource(ResourceType.DISK, 1024, Seq(reservationInfo), Some(disk))
      val resourceWithoutReservation = newScalarResource(ResourceType.DISK, 1024, None)

      // simple case: Only exact match contained

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation),
        usedResources = Seq(resourceWithReservation)
      ) should be(empty)

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithoutReservation),
        usedResources = Seq(resourceWithoutReservation)
      ) should be(empty)

      // ensure that the correct choice is made

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithoutReservation, resourceWithReservation),
        usedResources = Seq(resourceWithReservation)
      ) should be(Seq(resourceWithoutReservation))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation, resourceWithoutReservation),
        usedResources = Seq(resourceWithReservation)
      ) should be(Seq(resourceWithoutReservation))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation, resourceWithoutReservation),
        usedResources = Seq(resourceWithoutReservation)
      ) should be(Seq(resourceWithReservation))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithoutReservation, resourceWithReservation),
        usedResources = Seq(resourceWithoutReservation)
      ) should be(Seq(resourceWithReservation))

      // if there is no match, leave resources unchanged

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation),
        usedResources = Seq(resourceWithoutReservation)
      ) should be(Seq(resourceWithReservation))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation),
        usedResources = Seq(resourceWithoutReservation)
      ) should be(Seq(resourceWithReservation))
    }

    "fully consume mount disks" in {
      ResourceUtil.consumeScalarResource(
        newScalarResource(ResourceType.DISK, 1024.0, disk = Some(mountDisk(Some("/mnt/disk1")))),
        32.0
      ) shouldBe (None)
    }

    "consider reservation labels" in {
      val reservationInfo1 = ProtoBuilders.newResourceReservationInfo(ReservationInfo.Type.STATIC, "role", "principal")

      val reservationInfo2 = reservationInfo1.toBuilder
        .setLabels(Map("key" -> "value").asProtoLabels)
        .build

      val resourceWithReservation1 = newScalarResource(ResourceType.DISK, 1024, Seq(reservationInfo1))
      val resourceWithReservation2 = newScalarResource(ResourceType.DISK, 1024, Seq(reservationInfo2))

      // simple case: Only exact match contained

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation1),
        usedResources = Seq(resourceWithReservation1)
      ) should be(empty)

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation2),
        usedResources = Seq(resourceWithReservation2)
      ) should be(empty)

      // ensure that the correct choice is made

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation2, resourceWithReservation1),
        usedResources = Seq(resourceWithReservation1)
      ) should be(Seq(resourceWithReservation2))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation1, resourceWithReservation2),
        usedResources = Seq(resourceWithReservation1)
      ) should be(Seq(resourceWithReservation2))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation1, resourceWithReservation2),
        usedResources = Seq(resourceWithReservation2)
      ) should be(Seq(resourceWithReservation1))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation2, resourceWithReservation1),
        usedResources = Seq(resourceWithReservation2)
      ) should be(Seq(resourceWithReservation1))

      // if there is no match, leave resources unchanged

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation1),
        usedResources = Seq(resourceWithReservation2)
      ) should be(Seq(resourceWithReservation1))

      ResourceUtil.consumeResources(
        resources = Seq(resourceWithReservation1),
        usedResources = Seq(resourceWithReservation2)
      ) should be(Seq(resourceWithReservation1))
    }

    "format reservation into `displayResource` output" in {
      val reservationInfo = ProtoBuilders.newResourceReservationInfo(ReservationInfo.Type.STATIC, "role", "principal")
      val resource = newScalarResource(ResourceType.DISK, 1024, Some(reservationInfo))
      val resourceString = ResourceUtil.displayResources(Seq(resource), maxRanges = 10)
      resourceString should equal("disk(role, RESERVED for principal) 1024.0")
    }

    "show disk and reservation info in `displayResources`" in {
      val reservationInfo = ProtoBuilders.newResourceReservationInfo(ReservationInfo.Type.STATIC, "role", "principal")
      val disk = DiskInfo.newBuilder().setPersistence(Persistence.newBuilder().setId("persistenceId")).build()
      val resource = newScalarResource(ResourceType.DISK, 1024, Some(reservationInfo), Some(disk))
      val resourceString = ResourceUtil.displayResources(Seq(resource), maxRanges = 10)
      resourceString should equal("disk(role, RESERVED for principal, diskId persistenceId) 1024.0")
    }

    // in the middle
    portsTest(
      consumedResource = Seq(10 to 10),
      baseResource = Seq(5 to 15),
      expectedResult = Some(Seq(5 to 9, 11 to 15))
    )
    portsTest(
      consumedResource = Seq(10 to 11),
      baseResource = Seq(5 to 15),
      expectedResult = Some(Seq(5 to 9, 12 to 15))
    )
    portsTest(
      consumedResource = Seq(10 to 11),
      baseResource = Seq(5 to 15, 30 to 31),
      expectedResult = Some(Seq(5 to 9, 12 to 15, 30 to 31))
    )

    portsTest(consumedResource = Seq(), baseResource = Seq(5 to 15), expectedResult = Some(Seq(5 to 15)))

    portsTest(
      consumedResource = Seq(31084 to 31084),
      baseResource = Seq(31000 to 31096, 31098 to 32000),
      expectedResult = Some(Seq(31000 to 31083, 31085 to 31096, 31098 to 32000))
    )

    // overlapping smaller
    portsTest(consumedResource = Seq(2 to 5), baseResource = Seq(5 to 15), expectedResult = Some(Seq(6 to 15)))
    portsTest(consumedResource = Seq(2 to 6), baseResource = Seq(5 to 15), expectedResult = Some(Seq(7 to 15)))

    // overlapping bigger
    portsTest(consumedResource = Seq(15 to 20), baseResource = Seq(5 to 15), expectedResult = Some(Seq(5 to 14)))
    portsTest(consumedResource = Seq(14 to 20), baseResource = Seq(5 to 15), expectedResult = Some(Seq(5 to 13)))

    // not contained in base resource
    portsTest(consumedResource = Seq(5 to 15), baseResource = Seq(), expectedResult = None)
    portsTest(consumedResource = Seq(2 to 4), baseResource = Seq(5 to 15), expectedResult = Some(Seq(5 to 15)))
    portsTest(consumedResource = Seq(16 to 20), baseResource = Seq(5 to 15), expectedResult = Some(Seq(5 to 15)))

    scalarTest(consumedResource = 3, baseResource = 10, expectedResult = Some(10.0 - 3.0))
    scalarTest(consumedResource = 3, baseResource = 2, expectedResult = None)

    setResourceTest(
      consumedResource = Set("a", "b"),
      baseResource = Set("a", "b", "c"),
      expectedResult = Some(Set("c"))
    )
    setResourceTest(consumedResource = Set("a", "b", "c"), baseResource = Set("a", "b", "c"), expectedResult = None)
  }

  private[this] def setResourceTest(
      consumedResource: Set[String],
      baseResource: Set[String],
      expectedResult: Option[Set[String]]
  ): Unit = {

    s"consuming sets resource $consumedResource from $baseResource results in $expectedResult" in {
      val r1 = set(ResourceType.UNKNOWN("labels"), consumedResource)
      val r2 = set(ResourceType.UNKNOWN("labels"), baseResource)
      val r3 = expectedResult.map(set(ResourceType.UNKNOWN("labels"), _))
      val result = ResourceUtil.consumeResource(r2, r1)
      assert(result == r3)
    }
  }

  private[this] def set(resourceType: ResourceType, labels: Set[String]): Protos.Resource = {
    ProtoBuilders.newResource(
      resourceType.name,
      Protos.Value.Type.SET,
      ProtoBuilders.newResourceAllocationInfo("some-role"),
      set = labels.asProtoSet
    )
  }

  private[this] def portsTest(
      consumedResource: Seq[Range.Inclusive],
      baseResource: Seq[Range.Inclusive],
      expectedResult: Option[Seq[Range.Inclusive]]
  ): Unit = {

    s"consuming ports resource $consumedResource from $baseResource results in $expectedResult" in {
      val r1 = ports(consumedResource: _*)
      val r2 = ports(baseResource: _*)
      val r3 = expectedResult.map(ports(_: _*))
      val result = ResourceUtil.consumeResource(r2, r1)
      assert(result == r3)
    }
  }

  private[this] def ports(ranges: Range.Inclusive*): Protos.Resource = {
    ProtoBuilders.newResource(
      ResourceType.PORTS.name,
      Protos.Value.Type.RANGES,
      ProtoBuilders.newResourceAllocationInfo("some-role"),
      ranges = ProtoBuilders.newValueRanges(ranges.map(_.asProtoRange))
    )
  }

  private[this] def scalarTest(consumedResource: Double, baseResource: Double, expectedResult: Option[Double]): Unit = {
    s"consuming scalar resource $consumedResource from $baseResource results in $expectedResult" in {
      val r1 = newScalarResource(ResourceType.CPUS, consumedResource)
      val r2 = newScalarResource(ResourceType.CPUS, baseResource)
      val r3 = expectedResult.map(newScalarResource(ResourceType.CPUS, _))
      val result = ResourceUtil.consumeResource(r2, r1)
      assert(result == r3)
    }
  }
}
