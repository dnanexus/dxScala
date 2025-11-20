package dx.api

import Tags.{ApiTest, EdgeTest}
import dx.api.InstanceTypeDB.instanceTypeDBFormat
import dx.util.{JsUtils, Logger}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class InstanceTypeDBTest extends AnyFlatSpec with Matchers {

  // The original list is at:
  // https://github.com/dnanexus/nucleus/blob/master/commons/instance_types/assets/aws_instance_types.json
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
      ExecutionEnvironment("ubuntu", "24.04", Vector("0"))
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

  private def createTestInstance(name: String, memMB: Long, diskGB: Long): DxInstanceType = {
    val cpu = name.split("_").last.drop(1).toInt
    val gpu = name.toLowerCase.contains("gpu")

    DxInstanceType(name,
                   memMB,
                   diskGB,
                   cpu,
                   gpu,
                   ExecutionEnvironments,
                   Some(DiskType.SSD),
                   Some(1))
  }

  private def createInstanceTypeDB(instances: DxInstanceType*): InstanceTypeDB = {
    InstanceTypeDB(instances.map(instance => instance.name -> instance).toMap)
  }

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

  it should "defaultInstanceType. select the minimal preferred v2 instance when available" in {
    // Setup a DB where v2 is better than v1
    val db = createInstanceTypeDB(
        createTestInstance("mem1_ssd1_v2_x72", 144000, 1675), // v2: meets min requirements, but there is a better preferred v2
        createTestInstance("mem1_ssd1_v2_x16", 32000, 372), // Preferred v2: meets min requirements
        createTestInstance("mem1_ssd1_x2", 3766, 28), // V1
        createTestInstance("mem2_ssd2_gpu1_v2_x4", 16384, 235) // Non-preferred v2 (GPU)
    )

    db.defaultInstanceType.name shouldBe "mem1_ssd1_v2_x16"
  }

  it should "defaultInstanceType. select the minimal v1 instance if no preferred v2 exists" in {
    // Setup a DB with only v1 and non-preferred v2 instances
    val db = createInstanceTypeDB(
        createTestInstance("mem2_ssd2_gpu1_v2_x4", 16384, 235), // Non-preferred v2 (GPU)
        createTestInstance("mem1_ssd1_x2", 3766, 28), // V1
        createTestInstance("v2_fpga_v2", 8192, 100) // Another Non-preferred v2 (FPGA): third lowest rank
    )

    db.defaultInstanceType.name shouldBe "mem1_ssd1_x2"
  }

  it should "defaultInstanceType. select the minimal non-preferred v2 instance if no preferred v2 or v1 exists" in {
    // Setup a DB with only non-preferred v2 instances
    val db = createInstanceTypeDB(
        createTestInstance("mem2_ssd2_gpu1_v2_x4", 16384, 235), // Non-preferred v2 (GPU)
        createTestInstance("mem3_ssd2_fpga1_x24", 262144, 910) // Another Non-preferred v2 (FPGA)
    )

    db.defaultInstanceType.name shouldBe "mem2_ssd2_gpu1_v2_x4"
  }

  it should "defaultInstanceType. throw an exception if no eligible instances meet minimums" in {
    // MinMemory = 3072, MinCpu = 2
    val db = createInstanceTypeDB(
        createTestInstance("mem1_ssd1_x2", 3000, 28), // Fails min memory check
        createTestInstance("mem2_hdd2_x1", 3750, 397), // Fails min cpu check
        createTestInstance("test_instance_x2", 4000, 100) // Ignored instance name
    )

    // Expect exception
    val ex = intercept[Exception] {
      db.defaultInstanceType
    }
    ex.getMessage should include(
        "no instance types meet the minimal requirements memory >= 3072 AND cpu >= 2"
    )
  }

  it should "selectOptimal. work on large instances (JIRA-1258)" in {
    val db = createInstanceTypeDB(
        createTestInstance("mem3_ssd1_x32", 245751, 597),
        createTestInstance("mem4_ssd1_x128", 1967522, 3573)
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

  it should "selectOptimal. prefer v2 instances over v1's" in {
    val db = createInstanceTypeDB(
        createTestInstance("mem1_ssd1_v2_x4", 8000, 80),
        createTestInstance("mem1_ssd1_x4", 8000, 80)
    )

    db.selectOptimal(InstanceTypeRequest(minCpu = Some(4))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_v2_x4" =>
    }
  }

  it should "selectOptimal. upgrade to v2 when specifying system requirements with CPU" in {
    testDb.selectOptimal(InstanceTypeRequest(minCpu = Some(16))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_v2_x16" =>
    }
  }

  it should "selectOptimal. keep v1 instance because v2 is not available in the DB" in {
    testDb
      .selectOptimal(InstanceTypeRequest(minCpu = Some(2), minMemoryMB = Some(7000))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem2_ssd1_x2" =>
    }
  }

  it should "selectOptimal. respect requests for GPU instances" taggedAs EdgeTest in {
    val db = createInstanceTypeDB(
        createTestInstance("mem1_ssd1_v2_x4", 8000, 80),
        createTestInstance("mem1_ssd1_x4", 8000, 80),
        createTestInstance("mem3_ssd1_gpu_x8", 30000, 100)
    )

    db.selectOptimal(InstanceTypeRequest(minCpu = Some(4), gpu = Some(true))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem3_ssd1_gpu_x8" =>
    }

    // No non-GPU instance has 8 cpus
    db.selectOptimal(InstanceTypeRequest(minCpu = Some(8), gpu = Some(false))) shouldBe None
  }

  it should "selectByName. issue a warning if requested a v1 instance by ID but v2 is available" in {
    testDb.selectByName("mem1_ssd1_x16") should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_x16" =>
    }
  }

  it should "AWS region. Query returns correct pricing models for org and user" taggedAs ApiTest in {
    def instanceTypeFilter(instanceType: DxInstanceType): Boolean = {
      instanceType.os.exists(_.release == "24.04")
    }
    val userBilltoProject = dxApi.project("project-Fy9QqgQ0yzZbg9KXKP4Jz6Yq") // project name: dxCompiler_playground
    val db = InstanceTypeDB.create(userBilltoProject, instanceTypeFilter)

    db.instanceTypes.size shouldBe 133
    db.defaultInstanceType.name shouldBe "mem1_ssd1_v2_x2"
  }

  it should "OCI region. Query returns correct pricing models for org and user" taggedAs ApiTest in {
    def instanceTypeFilter(instanceType: DxInstanceType): Boolean = {
      instanceType.os.exists(_.release == "24.04")
    }
    val userBilltoProject = dxApi.project("project-J1q0ZK963q4JXY2Qqv8xvX5J") // project name: App_Assets_Ashburn_Internal
    val db = InstanceTypeDB.create(userBilltoProject, instanceTypeFilter)

    db.instanceTypes.size shouldBe 37
    db.defaultInstanceType.name shouldBe "oci:mem1_ssd1_v3i_x2"
  }

  it should "Azure region. Query returns correct pricing models for org and user" taggedAs ApiTest in {
    def instanceTypeFilter(instanceType: DxInstanceType): Boolean = {
      instanceType.os.exists(_.release == "24.04")
    }
    val userBilltoProject = dxApi.project("project-G24215Q9Vz71vv4b6Z3P6j84") // project name: App_Assets_Azure_Internal
    val db = InstanceTypeDB.create(userBilltoProject, instanceTypeFilter)

    db.instanceTypes.size shouldBe 22
    db.defaultInstanceType.name shouldBe "azure:mem1_ssd1_x2"
  }
}
