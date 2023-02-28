package dx.api

import Tags.{ApiTest, EdgeTest}
import dx.api.InstanceTypeDB.instanceTypeDBFormat
import dx.util.JsUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import dx.util.Logger

class InstanceTypeDBTest extends AnyFlatSpec with Matchers {

  // The original list is at:
  // https://github.com/dnanexus/nucleus/blob/master/commons/instance_types/aws_instance_types.json
  //
  // The g2,i2,x1 instances have been removed, because they are not
  // enabled for customers by default.  In addition, the PV (Paravirtual)
  // instances have been removed, because they work only on Ubuntu
  // 12.04.
  //
  // Removed the ssd2 instances, because they actually use EBS storage. A better
  // solution would be asking the platform for the available instances.
  private val instanceList: String =
    """{
        "mem2_ssd1_x2":       {"internalName": "m3.large", "cloudInstanceType": "m3.large", "traits": {"numCores":   2, "totalMemoryMB":    7225, "ephemeralStorageGB":   27, "rank": 6}},
        "mem2_ssd1_x4":       {"internalName": "m3.xlarge", "cloudInstanceType": "m3.xlarge", "traits": {"numCores":   4, "totalMemoryMB":   14785, "ephemeralStorageGB":   72, "rank": 19}},
        "mem2_ssd1_x8":       {"internalName": "m3.2xlarge", "cloudInstanceType": "m3.2xlarge", "traits": {"numCores":   8, "totalMemoryMB":   29905, "ephemeralStorageGB":  147, "rank": 34}},
        "mem1_ssd1_x2":       {"internalName": "c3.large", "cloudInstanceType": "c3.large", "traits": {"numCores":   2, "totalMemoryMB":    3766, "ephemeralStorageGB":   28, "rank": 2}},
        "mem1_ssd1_x4":       {"internalName": "c3.xlarge", "cloudInstanceType": "c3.xlarge", "traits": {"numCores":   4, "totalMemoryMB":    7225, "ephemeralStorageGB":   77, "rank": 12}},
        "mem1_ssd1_x8":       {"internalName": "c3.2xlarge", "cloudInstanceType": "c3.2xlarge", "traits": {"numCores":   8, "totalMemoryMB":   14785, "ephemeralStorageGB":  157, "rank": 25}},
        "mem1_ssd1_x16":      {"internalName": "c3.4xlarge", "cloudInstanceType": "c3.4xlarge", "traits": {"numCores":  16, "totalMemoryMB":   29900, "ephemeralStorageGB":  302, "rank": 39}},
        "mem1_ssd1_v2_x16":   {"internalName": "c5d.4xlarge_v2", "cloudInstanceType": "c5d.4xlarge", "traits": {"numCores":  16, "totalMemoryMB":   32000, "ephemeralStorageGB":  372, "rank": 40}},
        "mem1_ssd1_x32":      {"internalName": "c3.8xlarge", "cloudInstanceType": "c3.8xlarge", "traits": {"numCores":  32, "totalMemoryMB":   60139, "ephemeralStorageGB":  637, "rank": 53}},
        "mem3_ssd1_x2":       {"internalName": "r3.large", "cloudInstanceType": "r3.large", "traits": {"numCores":   2, "totalMemoryMB":   15044, "ephemeralStorageGB":   27, "rank": 9}},
        "mem3_ssd1_x4":       {"internalName": "r3.xlarge", "cloudInstanceType": "r3.xlarge", "traits": {"numCores":   4, "totalMemoryMB":   30425, "ephemeralStorageGB":   72, "rank": 22}},
        "mem3_ssd1_x8":       {"internalName": "r3.2xlarge", "cloudInstanceType": "r3.2xlarge", "traits": {"numCores":   8, "totalMemoryMB":   61187, "ephemeralStorageGB":  147, "rank": 37}},
        "mem3_ssd1_x16":      {"internalName": "r3.4xlarge", "cloudInstanceType": "r3.4xlarge", "traits": {"numCores":  16, "totalMemoryMB":  122705, "ephemeralStorageGB":  297, "rank": 50}},
        "mem3_ssd1_x32":      {"internalName": "r3.8xlarge", "cloudInstanceType": "r3.8xlarge", "traits": {"numCores":  32, "totalMemoryMB":  245751, "ephemeralStorageGB":  597, "rank": 64}}
}"""

  private val ExecutionEnvironments = Vector(
      ExecutionEnvironment("ubuntu", "20.04", Vector("0"))
  )

  // Create an available instance list based on a hard coded list
  private lazy val testDb: InstanceTypeDB = {
    val allInstances: Map[String, JsValue] = instanceList.parseJson.asJsObject.fields
    val db = allInstances.map {
      case (name, v) =>
        val fields: Map[String, JsValue] = v.asJsObject.fields
        val traits = fields("traits").asJsObject.fields
        val minMemoryMB = JsUtils.getInt(traits, "totalMemoryMB")
        val diskGB = JsUtils.getInt(traits, "ephemeralStorageGB")
        val cpu = JsUtils.getInt(traits, "numCores")
        val priceRank = JsUtils.getInt(traits, "rank")
        name -> DxInstanceType(name,
                               minMemoryMB,
                               diskGB,
                               cpu,
                               gpu = false,
                               Vector.empty,
                               None,
                               Some(priceRank))
    }
    InstanceTypeDB(db)
  }

  private val dxApi: DxApi = DxApi()(Logger.Quiet)

  it should "compare two instance types" in {
    // instances where all lhs resources are less than all rhs resources
    val c1 = testDb.compareByResources("mem1_ssd1_x2", "mem1_ssd1_x8")
    c1 should be < 0
    // instances where some resources are less and some are greater
    testDb.compareByResources("mem1_ssd1_x4", "mem3_ssd1_x2") shouldBe 1
    testDb.matchesOrExceedes("mem1_ssd1_x4", "mem3_ssd1_x2") shouldBe false
    // non existant instance
    assertThrows[Exception] {
      testDb.compareByResources("mem1_ssd2_x2", "ggxx") shouldBe 0
    }
  }

  it should "perform JSON serialization" in {
    val js = testDb.toJson
    val db2 = js.asJsObject.convertTo[InstanceTypeDB]
    testDb should equal(db2)
  }

  it should "pretty print" in {
    // Test pretty printing
    Logger.get.ignore(testDb.prettyFormat())
  }

  it should "work on large instances (JIRA-1258)" in {
    val db = InstanceTypeDB(
        Map(
            "mem3_ssd1_x32" -> DxInstanceType(
                "mem3_ssd1_x32",
                245751,
                32,
                597,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            ),
            "mem4_ssd1_x128" -> DxInstanceType(
                "mem4_ssd1_x128",
                1967522,
                128,
                3573,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(2)
            )
        )
    )

    db.selectOptimal(
        InstanceTypeRequest(minMemoryMB = Some(239 * 1024), minDiskGB = Some(18), minCpu = Some(32))
    ) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem3_ssd1_x32" =>
    }
    db.selectOptimal(
        InstanceTypeRequest(minMemoryMB = Some(240 * 1024), minDiskGB = Some(18), minCpu = Some(32))
    ) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem4_ssd1_x128" =>
    }
  }

  it should "prefer v2 instances over v1's" in {
    val db = InstanceTypeDB(
        Map(
            "mem1_ssd1_v2_x4" -> DxInstanceType(
                "mem1_ssd1_v2_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            ),
            "mem1_ssd1_x4" -> DxInstanceType(
                "mem1_ssd1_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            )
        )
    )

    db.selectOptimal(InstanceTypeRequest(minCpu = Some(4))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_v2_x4" =>
    }
  }

  it should "upgrade to v2 if available when specifying system requirements" in {
    val db = InstanceTypeDB(
        Map(
            "mem1_ssd1_v2_x4" -> DxInstanceType(
                "mem1_ssd1_v2_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            ),
            "mem1_ssd1_x4" -> DxInstanceType(
                "mem1_ssd1_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            )
        )
    )

    db.selectOptimal(InstanceTypeRequest(minCpu = Some(4))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_v2_x4" =>
    }
  }

  it should "respect requests for GPU instances" taggedAs EdgeTest in {
    val db = InstanceTypeDB(
        Map(
            "mem1_ssd1_v2_x4" -> DxInstanceType(
                "mem1_ssd1_v2_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            ),
            "mem1_ssd1_x4" -> DxInstanceType(
                "mem1_ssd1_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1)
            ),
            "mem3_ssd1_gpu_x8" -> DxInstanceType(
                "mem3_ssd1_gpu_x8",
                30000,
                100,
                8,
                gpu = true,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(2)
            )
        )
    )

    db.selectOptimal(InstanceTypeRequest(minCpu = Some(4), gpu = Some(true))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem3_ssd1_gpu_x8" =>
    }

    // No non-GPU instance has 8 cpus
    db.selectOptimal(InstanceTypeRequest(minCpu = Some(8), gpu = Some(false))) shouldBe None
  }

  it should "Query returns correct pricing models for org and user" taggedAs ApiTest in {
    // Instance type filter:
    // - Instance must support Ubuntu.
    // - Instance is not an FPGA instance.
    // - Instance does not have local HDD storage (those are older instance types).
    def instanceTypeFilter(instanceType: DxInstanceType): Boolean = {
      instanceType.os.exists(_.release == "20.04") &&
      !instanceType.diskType.contains(DiskType.HDD) &&
      !instanceType.name.contains("fpga")
    }
    val userBilltoProject = dxApi.project("project-Fy9QqgQ0yzZbg9KXKP4Jz6Yq") // project name: dxCompiler_public_test
    val db = InstanceTypeDB.create(userBilltoProject, instanceTypeFilter)
    db.instanceTypes.size shouldBe 69
  }
}
