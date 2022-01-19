/**
Match the runtime WDL requirements to an machine instance supported by the platform.

There there may be requirements for memory, disk-space, and number of
cores. We interpret these as minimal requirements and try to choose a
good and inexpensive instance from the available list.

For example, a WDL task could have a runtime section like the following:
runtime {
    memory: "14 GB"
    cpu: "16"
    disks: "local-disk " + disk_size + " HDD"
}


Representation of a platform instance, how much resources it has, and
how much it costs to run.

resource    measurement units
--------    -----------------
memory      MB of RAM
disk        GB of disk space, hard drive or flash drive
cpu         1 per core
price       comparative price

  */
package dx.api

import dx.api
import dx.util.{Enum, JsUtils, Logger}
import dx.util.Enum.enumFormat
import spray.json.{RootJsonFormat, _}

import scala.collection.immutable.ListMap

object DiskType extends Enum {
  type DiskType = Value
  val HDD, SSD = Value
  private val Aliases = Map("SDD" -> SSD)

  override protected lazy val nameToValue: ListMap[String, api.DiskType.Value] = {
    ListMap.from(values.map(x => x.toString -> x).toVector ++ Aliases.toVector)
  }
}

case class ExecutionEnvironment(distribution: String,
                                release: String,
                                versions: Vector[String] = Vector(
                                    ExecutionEnvironment.DefaultVersion
                                ))

object ExecutionEnvironment {
  val DefaultVersion = "0"
}

/**
  * Parameters for an instance type query. Any parameters that are None
  * will not be considered in the query.
  */
case class InstanceTypeRequest(dxInstanceType: Option[String] = None,
                               minMemoryMB: Option[Long] = None,
                               maxMemoryMB: Option[Long] = None,
                               minDiskGB: Option[Long] = None,
                               maxDiskGB: Option[Long] = None,
                               diskType: Option[DiskType.DiskType] = None,
                               minCpu: Option[Long] = None,
                               maxCpu: Option[Long] = None,
                               gpu: Option[Boolean] = None,
                               os: Option[ExecutionEnvironment] = None,
                               optional: Boolean = false) {
  override def toString: String = {
    s"""memory=(${minMemoryMB},${maxMemoryMB}) disk=(${minDiskGB},${maxDiskGB}) diskType=${diskType} 
       |cores=(${minCpu},${maxCpu}) gpu=${gpu} os=${os} instancetype=${dxInstanceType}
       |optional=${optional}""".stripMargin.replaceAll("\n", " ")
  }

  def hasMaxBounds: Boolean = {
    maxMemoryMB.isDefined || maxDiskGB.isDefined || maxCpu.isDefined
  }

  def isEmpty: Boolean = {
    this.productIterator.forall {
      case o: Option[_] if o.nonEmpty => false
      case _                          => true
    }
  }
}

object InstanceTypeRequest {
  lazy val empty: InstanceTypeRequest = InstanceTypeRequest()
}

/**
  * DNAnexus instance type.
  * @param name canonical instance type name, e.g. mem1_ssd1_x4
  * @param memoryMB available RAM in MB
  * @param diskGB available disk in GB
  * @param cpu number of CPU cores
  * @param gpu whether there is at least one GPU
  * @param os Vector of execution environments available, e.g.
  *           [(Ubuntu, 16.04, 1), (Ubuntu, 20.04, 0)]
  * @param diskType SSD or HDD
  * @param priceRank rank of this instance type's price vs the
  *                  other instance types in the database. Rank
  *                  begins at 1 for the cheapest instance. Two
  *                  instances with the same price will have
  *                  different (consecutive) ranks assigned at
  *                  random.
  */
case class DxInstanceType(name: String,
                          memoryMB: Long,
                          diskGB: Long,
                          cpu: Int,
                          gpu: Boolean,
                          os: Vector[ExecutionEnvironment],
                          diskType: Option[DiskType.DiskType] = None,
                          priceRank: Option[Int] = None)
    extends Ordered[DxInstanceType] {

  /**
    * Returns true if this instance type satisfies the requirements of `query`,
    * which happens if 1) this instance type has the same name as specified in
    * the query, or 2) all of its resources are at least as large as the
    * minimums in the query, and also less than the maximums if `enforceMaxBounds`
    * is true.
    */
  def satisfies(query: InstanceTypeRequest, enforceMaxBounds: Boolean = false): Boolean = {
    if (query.dxInstanceType.contains(name)) {
      true
    } else {
      query.minMemoryMB.forall(_ <= memoryMB) &&
      query.minDiskGB.forall(_ <= diskGB) &&
      query.minCpu.forall(_ <= cpu) &&
      query.gpu.forall(_ == gpu) &&
      query.os.forall(queryOs => os.contains(queryOs)) && (
          !enforceMaxBounds || (
              query.maxMemoryMB.forall(_ >= memoryMB) &&
                query.maxDiskGB.forall(_ >= diskGB) &&
                query.maxCpu.forall(_ >= cpu)
          )
      )
    }
  }

  def compareByPrice(that: DxInstanceType): Int = {
    // compare by price
    (this.priceRank, that.priceRank) match {
      case (Some(p1), Some(p2)) => p1.compare(p2)
      case _                    => 0
    }
  }

  private def computeResourceDeltas(that: DxInstanceType, fuzzy: Boolean): Vector[Double] = {
    val (memDelta: Double, diskDelta: Double) = if (fuzzy) {
      ((this.memoryMB.toDouble / DxInstanceType.MemoryNormFactor) -
         (that.memoryMB.toDouble / DxInstanceType.MemoryNormFactor),
       (this.diskGB.toDouble / DxInstanceType.DiskNormFactor) -
         (that.diskGB.toDouble / DxInstanceType.DiskNormFactor))
    } else {
      ((this.memoryMB - that.memoryMB).toDouble, (this.diskGB - that.diskGB).toDouble)
    }
    val cpuDelta: Double = this.cpu - that.cpu
    val gpuDelta: Double = (if (this.gpu) 1 else 0) - (if (that.gpu) 1 else 0)
    Vector(cpuDelta, memDelta, gpuDelta, diskDelta)
  }

  /**
    * Do all of the resources of `this` match or exceed those of `that`?
    */
  def matchesOrExceeded(that: DxInstanceType, fuzzy: Boolean = true): Boolean = {
    computeResourceDeltas(that, fuzzy).sortWith(_ < _).head >= 0
  }

  /**
    * Compare based on resource sizes. This is a partial ordering.
    *
    * If `this` has some resources that are greater than and some that
    * are less than `that`, the comparison is done based on the following
    * priority:
    * 1. cpu
    * 2. memory
    * 3. gpu
    * 4. disk space
    *
    * Optionally, adds some fuzziness to the comparison, because the instance
    * types don't have the exact memory and disk space that you would
    * expect (e.g. mem1_ssd1_x2 has less disk space than mem2_ssd1_x2) -
    * round down memory and disk sizes, to make the comparison insensitive to
    * minor differences.
    *
    * @param that DxInstanceType
    * @param fuzzy whether to do fuzz or exact comparison
    * @return
    * @example If A has more memory, disk space, and cores than B, then B < A.
    */
  def compareByResources(that: DxInstanceType, fuzzy: Boolean = true): Int = {
    val resources = computeResourceDeltas(that, fuzzy)
    resources.distinct match {
      // all resources are equal
      case Vector(0.0) => 0
      case d =>
        d.sortWith(_ < _) match {
          // all resources in this are greater than that
          case dSorted if dSorted.head >= 0.0 => 1
          // all resources in that are greater than this
          case dSorted if dSorted.last <= 0.0 => -1
          // some resources are greater and others less - do the comparison hierarchically
          case _ =>
            resources
              .collectFirst {
                case r if r > 0 => 1
                case r if r < 0 => -1
              }
              .getOrElse(throw new Exception("expected at least one non-zero delta"))
        }
    }
  }

  /**
    * Compare instances based on version (v1 vs v2) -
    * v2 instances are always better than v1 instances.
    */
  def compareByType(that: DxInstanceType): Int = {
    def typeVersion(name: String): Int = if (name contains DxInstanceType.Version2Suffix) 2 else 1
    typeVersion(this.name).compareTo(typeVersion(that.name))
  }

  override def compare(that: DxInstanceType): Int = {
    // if price ranks are available, choose the cheapest instance
    val costCmp = compareByPrice(that)
    if (costCmp != 0) {
      costCmp
    } else {
      // otherwise choose one with minimal resources
      val resCmp = compareByResources(that)
      if (resCmp != 0) {
        resCmp
      } else {
        // all else being equal, choose v2 instances over v1
        -compareByType(that)
      }
    }
  }
}

object DxInstanceType extends DefaultJsonProtocol {
  // support automatic conversion to/from JsValue
  implicit val diskTypeFormat: RootJsonFormat[DiskType.DiskType] = enumFormat(DiskType)
  implicit val execEnvFormat: RootJsonFormat[ExecutionEnvironment] = jsonFormat3(
      ExecutionEnvironment.apply
  )
  implicit val dxInstanceTypeFormat: RootJsonFormat[DxInstanceType] = jsonFormat8(
      DxInstanceType.apply
  )
  val Version2Suffix = "_v2"
  val MemoryNormFactor: Double = 1024.0
  val DiskNormFactor: Double = 16.0
}

case class InstanceTypeDB(instanceTypes: Map[String, DxInstanceType]) {
  // The cheapest available instance, this is normally also the smallest.
  // If there are multiple equally good instance types, we just pick one at random.
  private def selectMinimalInstanceType(
      instanceTypes: Iterable[DxInstanceType]
  ): Option[DxInstanceType] = {
    instanceTypes.toVector.sortWith(_ < _).headOption
  }

  /**
    * The default instance type for this database - the cheapest instance
    * type meeting minimal requirements. Used to run WDL expression
    * processing, launch jobs, etc.
    */
  lazy val defaultInstanceType: DxInstanceType = {
    val (v2InstanceTypes, v1InstanceTypes) = instanceTypes.values
      .filter { instanceType =>
        !instanceType.name.contains("test") &&
        instanceType.memoryMB >= InstanceTypeDB.MinMemory &&
        instanceType.cpu >= InstanceTypeDB.MinCpu
      }
      .partition(_.name.contains(DxInstanceType.Version2Suffix))
    // prefer v2 instance types
    selectMinimalInstanceType(v2InstanceTypes)
      .orElse(selectMinimalInstanceType(v1InstanceTypes))
      .getOrElse(
          throw new Exception(
              s"""no instance types meet the minimal requirements memory >= ${InstanceTypeDB.MinMemory} 
                 |AND cpu >= ${InstanceTypeDB.MinCpu}""".stripMargin.replaceAll("\n", " ")
          )
      )
  }

  def selectAll(query: InstanceTypeRequest,
                enforceMaxBounds: Boolean = false): Iterable[DxInstanceType] = {
    val q = if (query.diskType.contains(DiskType.HDD)) {
      // we are ignoring disk type - only SSD instances are considered usable
      Logger.get.warning(
          s"dxCompiler only utilizes SSD instance types - ignoring request for HDD instance ${query}"
      )
      query.copy(diskType = None)
    } else {
      query
    }
    instanceTypes.values.filter(x => x.satisfies(q, enforceMaxBounds))
  }

  /**
    * From among all the instance types that satisfy the query, selects the
    * optimal one, where optimal is defined as cheapest if the price list is
    * available, otherwise ...
    */
  def selectOptimal(query: InstanceTypeRequest,
                    enforceMaxBounds: Boolean = false): Option[DxInstanceType] = {
    val optimal = selectMinimalInstanceType(selectAll(query, enforceMaxBounds = true))
    if (optimal.isEmpty && !enforceMaxBounds && query.hasMaxBounds) {
      selectMinimalInstanceType(selectAll(query))
    } else {
      optimal
    }
  }

  def selectByName(name: String): Option[DxInstanceType] = {
    if (instanceTypes.contains(name)) {
      return instanceTypes.get(name)
    }
    val nameWithoutHdd = name.replace("hdd", "ssd")
    if (instanceTypes.contains(nameWithoutHdd)) {
      Logger.get.warning(
          s"dxCompiler only utilizes SSD instance types - ignoring request for HDD instance ${nameWithoutHdd}"
      )
      return instanceTypes.get(nameWithoutHdd)
    }
    None
  }

  /**
    * Query the database. If `query.dxInstanceType` is set, the query will
    * return the specified instance type if it is in the database, otherwise it
    * will return None unless `force=true`. Falls back to querying the database
    * by requirements and returns the cheapest instance type that meets all
    * requirements, if any.
    * @param query the query
    * @param enforceName whether to fall back to querying by requirements if the
    *                    instance type name is set but is not found in the database.
    * @param enforceMaxBounds whether to return None if there are no instance types
    *                        that satisfy both the min and max criteria, even if
    *                        there is one that satisfies only the min criteria.
    * @return
    */
  def get(query: InstanceTypeRequest,
          enforceName: Boolean = true,
          enforceMaxBounds: Boolean = false): Option[DxInstanceType] = {
    if (query.dxInstanceType.nonEmpty) {
      selectByName(query.dxInstanceType.get).orElse {
        if (enforceName) {
          None
        } else {
          selectOptimal(query, enforceMaxBounds)
        }
      }
    } else {
      selectOptimal(query, enforceMaxBounds)
    }
  }

  def apply(query: InstanceTypeRequest,
            enforceName: Boolean = false,
            enforceMaxBounds: Boolean = false): DxInstanceType = {
    get(query, enforceName, enforceMaxBounds) match {
      case Some(instanceType: DxInstanceType) => instanceType
      case None if query.optional             => defaultInstanceType
      case _ =>
        throw new Exception(s"No instance types found that satisfy query ${query}")
    }
  }

  // check if instance type A is smaller or equal in requirements to
  // instance type B
  def compareByResources(name1: String, name2: String, fuzzy: Boolean = false): Int = {
    Vector(name1, name2).map(instanceTypes.get) match {
      case Vector(Some(i1), Some(i2)) =>
        i1.compareByResources(i2, fuzzy = fuzzy)
      case _ =>
        throw new Exception(
            s"cannot compare instance types ${name1}, ${name2} - at least one is not in the database"
        )
    }
  }

  def matchesOrExceedes(name1: String, name2: String, fuzzy: Boolean = false): Boolean = {
    Vector(name1, name2).map(instanceTypes.get) match {
      case Vector(Some(i1), Some(i2)) =>
        i1.matchesOrExceeded(i2, fuzzy = fuzzy)
      case _ =>
        throw new Exception(
            s"cannot compare instance types ${name1}, ${name2} - at least one is not in the database"
        )
    }
  }

  /**
    * Formats the database as a prettified String. Instance types are sorted.
    * @return
    */
  def prettyFormat(): String = {
    instanceTypes.values.toVector.sortWith(_ < _).toJson.prettyPrint
  }
}

object InstanceTypeDB extends DefaultJsonProtocol {
  val MinMemory: Long = 3 * 1024
  val MinCpu: Int = 2
  val CpuKey = "numCores"
  val MemoryKey = "totalMemoryMB"
  val DiskKey = "ephemeralStorageGB"
  val OsKey = "os"
  val DistributionKey = "distribution"
  val ReleaseKey = "release"
  val VersionKey = "version"
  val RankKey = "rank"
  val GpuSuffix = "_gpu"
  val SsdSuffix = "_ssd"
  val HddSuffix = "_hdd"

  // support automatic conversion to/from JsValue
  implicit val instanceTypeDBFormat: RootJsonFormat[InstanceTypeDB] =
    new RootJsonFormat[InstanceTypeDB] {
      override def write(obj: InstanceTypeDB): JsValue = {
        obj.instanceTypes.toJson
      }

      override def read(json: JsValue): InstanceTypeDB = {
        InstanceTypeDB(json.convertTo[Map[String, DxInstanceType]])
      }
    }

  private def parseInstanceTypes(jsv: JsValue): Map[String, DxInstanceType] = {
    // convert to a list of DxInstanceTypes, with prices set to zero
    jsv.asJsObject.fields.map {
      case (name, jsValue) =>
        val numCores = JsUtils.getInt(jsValue, Some(CpuKey))
        val memoryMB = JsUtils.getInt(jsValue, Some(MemoryKey))
        val diskSpaceGB = JsUtils.getInt(jsValue, Some(DiskKey))
        val os = JsUtils.getValues(jsValue, Some(OsKey)).map {
          case obj: JsObject =>
            val distribution = JsUtils.getString(obj, Some(DistributionKey))
            val release = JsUtils.getString(obj, Some(ReleaseKey))
            val versions = obj.fields.get(VersionKey) match {
              case Some(JsArray(array)) => array.map(JsUtils.getString(_))
              case None                 => Vector(ExecutionEnvironment.DefaultVersion)
              case other =>
                throw new Exception(s"expected 'version' to be a string, not ${other}")
            }
            ExecutionEnvironment(distribution, release, versions)
          case other =>
            throw new Exception(s"invalid os specification ${other}")
        }
        // disk type and gpu details aren't reported by the API, so we
        // have to determine them from the instance type name
        val diskType = name.toLowerCase match {
          case s if s.contains(SsdSuffix) => Some(DiskType.SSD)
          case s if s.contains(HddSuffix) => Some(DiskType.HDD)
          case _                          => None
        }
        val gpu = name.toLowerCase.contains(GpuSuffix)
        val priceRank = JsUtils.getOptionalInt(jsValue, RankKey).orElse {
          Logger.get.warning(s"price rank is not available for instance type ${name}")
          None
        }
        name -> DxInstanceType(name, memoryMB, diskSpaceGB, numCores, gpu, os, diskType, priceRank)
    }
  }

  /**
    * Query the platform for the available instance types in this project.
    * @param dxProject the project from which to get the available instance types
    * @param instanceTypeFilter function used to filter instance types
    */
  def create(dxProject: DxProject,
             instanceTypeFilter: DxInstanceType => Boolean): InstanceTypeDB = {
    val availableInstanceTypes = parseInstanceTypes(
        dxProject
          .describe(Set(Field.AvailableInstanceTypes))
          .availableInstanceTypes
          .getOrElse(
              throw new Exception(
                  s"could not retrieve available instance types for project ${dxProject}"
              )
          )
    ).filter {
      case (_, instanceType) => instanceTypeFilter(instanceType)
    }
    InstanceTypeDB(availableInstanceTypes)
  }
}
