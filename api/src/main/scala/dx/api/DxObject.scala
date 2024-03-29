package dx.api

import dx.api.DxPath.{DxScheme, DxUriPrefix}
import spray.json._
import dx.util.Enum

import java.net.URI

// Extra fields for describe

// scalafmt setting to prevent a "Search state exploded" issue
// format: off
object Field extends Enum {
  type Field = Value
  val Access, Analysis, App, Applet, ArchivalState, AvailableInstanceTypes, BillTo, Categories,
      Created, DatabaseName, Description, Details, DependsOn, DeveloperNotes, DxInstanceClass, 
      Endpoint, Engine, EngineVersion, Executable, ExecutableName, Folder,
      Hidden, Id, IgnoreReuse, Input, Inputs, InputSpec, InstanceType, Modified, Name, Output,
      Outputs, OutputSpec, ParentJob, Parts, PricingModelsByRegion, Port, Project, Properties, Region,
      RunSpec, Size, Stages, State, Status, StatusAsOf, Summary, Tags, Title, Types, UniqueDatabaseName,
      Version, TreeTurnaroundTimeThreshold = Value
}
// format: on

trait DxObjectDescribe {
  val id: String
  val name: String
  val created: Long
  val modified: Long
  val properties: Option[Map[String, String]]
  val details: Option[JsValue]

  def getCreationDate: java.util.Date = new java.util.Date(created)

  def containsAll(fields: Set[Field.Value]): Boolean = ???
}

trait DxObject {
  def id: String

  def describe(fields: Set[Field.Value] = Set.empty): DxObjectDescribe
}

object DxObject {
  def parseJsonTags(tags: JsValue): Set[String] = {
    tags match {
      case JsArray(array) =>
        array.map {
          case JsString(tag) => tag
          case other         => throw new Exception(s"invalid tag ${other}")
        }.toSet
      case other => throw new Exception(s"unexpected tags value ${other}")
    }
  }

  def parseJsonProperties(props: JsValue): Map[String, String] = {
    props.asJsObject.fields.map {
      case (k, JsString(v)) => k -> v
      case (_, _) =>
        throw new Exception(s"malformed JSON properties ${props}")
    }
  }

  def maybeSpecifyProject(project: Option[DxProject]): Map[String, JsValue] = {
    project match {
      case None =>
        // we don't know the project.
        Map.empty
      case Some(p) =>
        // We know the project, this makes the search more efficient.
        Map("project" -> JsString(p.id))
    }
  }

  def requestFields(fields: Set[Field.Value]): JsValue = {
    val fieldStrings = fields.map {
      case Field.Access                      => "access"
      case Field.Analysis                    => "analysis"
      case Field.App                         => "app"
      case Field.Applet                      => "applet"
      case Field.ArchivalState               => "archivalState"
      case Field.AvailableInstanceTypes      => "availableInstanceTypes"
      case Field.BillTo                      => "billTo"
      case Field.Categories                  => "categories"
      case Field.Created                     => "created"
      case Field.DatabaseName                => "databaseName"
      case Field.DependsOn                   => "dependsOn"
      case Field.Description                 => "description"
      case Field.DeveloperNotes              => "developerNotes"
      case Field.Details                     => "details"
      case Field.DxInstanceClass             => "dxInstanceClass"
      case Field.Endpoint                    => "endpoint"
      case Field.Engine                      => "engine"
      case Field.EngineVersion               => "engineVersion"
      case Field.Executable                  => "executable"
      case Field.ExecutableName              => "executableName"
      case Field.Folder                      => "folder"
      case Field.Id                          => "id"
      case Field.Hidden                      => "hidden"
      case Field.IgnoreReuse                 => "ignoreReuse"
      case Field.Input                       => "input"
      case Field.Inputs                      => "inputs"
      case Field.InputSpec                   => "inputSpec"
      case Field.InstanceType                => "instanceType"
      case Field.Modified                    => "modified"
      case Field.Name                        => "name"
      case Field.Output                      => "output"
      case Field.Outputs                     => "outputs"
      case Field.OutputSpec                  => "outputSpec"
      case Field.ParentJob                   => "parentJob"
      case Field.Parts                       => "parts"
      case Field.Port                        => "port"
      case Field.PricingModelsByRegion       => "pricingModelsByRegion"
      case Field.Project                     => "project"
      case Field.Properties                  => "properties"
      case Field.Region                      => "region"
      case Field.RunSpec                     => "runSpec"
      case Field.Size                        => "size"
      case Field.Stages                      => "stages"
      case Field.State                       => "state"
      case Field.Status                      => "status"
      case Field.StatusAsOf                  => "statusAsOf"
      case Field.Summary                     => "summary"
      case Field.Tags                        => "tags"
      case Field.Title                       => "title"
      case Field.Types                       => "types"
      case Field.UniqueDatabaseName          => "uniqueDatabaseName"
      case Field.Version                     => "version"
      case Field.TreeTurnaroundTimeThreshold => "treeTurnaroundTimeThreshold"
    }.toVector
    val m: Map[String, JsValue] = fieldStrings.map { x =>
      x -> JsTrue
    }.toMap
    JsObject(m)
  }
}

trait DxDataObject extends DxObject {
  val project: Option[DxProject]
}

object DxDataObject {

  /**
    * Formats a file ID with optional project name or ID as a dx:// URI.
    */
  def format(objectId: String, project: Option[String]): String = {
    project
      .map(proj => new URI(DxScheme, s"${proj}:${objectId}", null, null, null).toString)
      .getOrElse(s"${DxUriPrefix}${objectId}")
  }
}

// Objects that can be run on the platform. These are apps, applets, and workflows.
trait DxExecutable extends DxObject

// execution priority
object Priority extends Enum {
  type Priority = Value
  val Normal, High = Value
}

// Actual executions on the platform. There are jobs and analyses
trait DxExecution extends DxObject {
  val id: String
  val project: Option[DxProject]
}

/**
  * DxDataObject that caches its description.
  * @tparam T type of DxObjectDescribe - must override `containsAll`
  */
abstract class CachingDxObject[T <: DxObjectDescribe] extends DxObject {
  private var cachedDesc: Option[T] = None

  def hasCachedDesc: Boolean = cachedDesc.nonEmpty

  def cacheDescribe(desc: DxObjectDescribe): Unit = {
    cachedDesc = Some(desc.asInstanceOf[T])
  }

  def describe(fields: Set[Field.Value] = Set.empty): T = {
    // only call describe if there is not a cached value,
    // or if the cached value does not contain all the
    // required fields
    if (cachedDesc.isEmpty || !cachedDesc.get.containsAll(fields)) {
      cachedDesc = Some(describeNoCache(fields))
    }
    cachedDesc.get
  }

  def describeNoCache(fields: Set[Field.Value]): T
}
