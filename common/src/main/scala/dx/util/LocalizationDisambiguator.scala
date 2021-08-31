package dx.util

import dx.util.CollectionUtils.IterableOnceExtensions
import dx.util.LoggerProtocol._
import spray.json._

import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.util.UUID

trait LocalizationDisambiguator {

  /**
    * Returns a unique local path for a single source file/directory.
    * @param fileNode the `FileSource to localize`
    * @param sourceContainer the source container of the file/directory (e.g. the
    *                        directory/folder it is coming from) - this must be
    *                        globally unique, to disambiguate between two identically
    *                        named folders in different file systems. All files from
    *                        from the same source container are localized to the
    *                        same directory.
    * @param version an optional file version, in the case that the source file system
    *                uses versioning
    * @param localizationDir optional directory where files must be localized;
    *                        and exception is thrown if there is a name collision
    */
  def getLocalPathForSource(fileNode: FileSource,
                            sourceContainer: String,
                            version: Option[String] = None,
                            localizationDir: Option[Path] = None): Path

  /**
    * Returns a unique local path for a single source file/directory.
    * @param fileSource `AddressableFileSource` to localize
    * @param localizationDir optional directory where files must be localized;
    *                        and exception is thrown if there is a name collision
    */
  def getLocalPath(fileSource: AddressableFileSource, localizationDir: Option[Path] = None): Path

  /**
    * Returns a mapping of file/directory source to unique local path.
    * This method is only meant to be called a single time for a given instance.
    * @param fileSources `AddressableFileSource`s to localize
    * @param localizationDir optional directory where files must be localized;
    *                        and exception is thrown if there is a name collision
    */
  def getLocalPaths[T <: AddressableFileSource](fileSources: Iterable[T],
                                                localizationDir: Option[Path] = None): Map[T, Path]

  /**
    * Convert LocalizationDisambiguator state to JSON.
    */
  def toJson: JsValue
}

/**
  * Localizes a file according to the rules in the spec:
  * https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#task-input-localization.
  * - two input files with the same name must be located separately, to avoid name collision
  * - two input files that originated in the same storage directory must also be localized into
  *   the same directory for task execution
  *
  * @param rootDir the root dir - files are localize to subdirectories under this directory
  * @param separateDirsBySource whether to always separate files from each source dir into
  *                             separate target dirs (true), or to minimize the number of
  *                              dirs used by putting all files in a single directory by default
  *                              but create additional directories to avoid name collision. This
  *                              is ignored when using the single file functions
  *                              (`getLocalPathForSource` and `getLocalPath`) because we have to
  *                              keep together files from the same source folder and there's no way
  *                              to know in advance if there will be another file from the same source
  *                              folder as this file and whether it will have a name collision.
  * @param createDirs whether to create the directories - set to true unless you are just using
  *                   this class to create unique names for files that will be synced e.g. using
  *                   a FUSE filesystem.
  * @param subdirPrefix prefix to add to localization dirs
  * @param disambiguationDirLimit max number of disambiguation subdirs that can be created
  * @param sourceToTarget mapping from source file container and version to local directory - this
  *                       ensures that files that were originally from the same directory are
  *                       localized to the same target directory
  * @param disambiguationDirs Set of disambiguation dirs that have been created
  * @param localizedPaths Set of paths that should already exist locally
  */
case class SafeLocalizationDisambiguator(
    rootDir: Path,
    separateDirsBySource: Boolean = false,
    createDirs: Boolean = true,
    subdirPrefix: String = "input",
    disambiguationDirLimit: Int = 200,
    logger: Logger = Logger.get
)(
    private var sourceToTarget: Map[(String, Option[String]), Path] = Map.empty,
    private var disambiguationDirs: Set[Path] = Set.empty,
    private var localizedPaths: Set[Path] = Set.empty
) extends LocalizationDisambiguator {

  override def toJson: JsValue = {
    JsObject(
        "rootDir" -> JsString(rootDir.toString),
        "separateDirsBySource" -> JsBoolean(separateDirsBySource),
        "createDirs" -> JsBoolean(createDirs),
        "subdirPrefix" -> JsString(subdirPrefix),
        "disambiguationDirLimit" -> JsNumber(disambiguationDirLimit),
        "logger" -> logger.toJson,
        "sourceToTarget" -> JsArray(
            sourceToTarget.map {
              case ((container, version), path) =>
                JsObject(
                    Vector(Some("container" -> JsString(container)),
                           version.map(ver => "verison" -> JsString(ver)),
                           Some("path" -> JsString(path.toString))).flatten.toMap
                )
            }.toVector
        ),
        "disambiguationDirs" -> JsArray(
            disambiguationDirs.map(path => JsString(path.toString)).toVector
        ),
        "localizedPaths" -> JsArray(localizedPaths.map(path => JsString(path.toString)).toVector)
    )
  }

  def getLocalizedPaths: Set[Path] = localizedPaths

  def getTargetDir(source: AddressableFileSource): Option[Path] = {
    sourceToTarget.get((source.container, source.version))
  }

  private def exists(path: Path): Boolean = {
    if (localizedPaths.contains(path)) {
      true
    } else if (Files.exists(path)) {
      localizedPaths += path
      true
    } else {
      false
    }
  }

  private def canCreateDisambiguationDir: Boolean = {
    disambiguationDirs.size < disambiguationDirLimit
  }

  private def createDisambiguationDir: Path = {
    val newDir = if (createDirs) {
      val newDir = Files.createTempDirectory(rootDir, subdirPrefix)
      // we should never get a collision according to the guarantees of
      // Files.createTempDirectory, but we check anyway
      if (disambiguationDirs.contains(newDir)) {
        throw new Exception(s"collision with existing dir ${newDir}")
      }
      newDir
    } else {
      // try random directory names until we find one that's not used
      Iterator
        .continually(UUID.randomUUID)
        .collectFirstDefined { u =>
          val newDir = rootDir.resolve(s"${subdirPrefix}${u.toString}")
          if (disambiguationDirs.contains(newDir)) {
            None
          } else {
            Some(newDir)
          }
        }
        .get
    }
    disambiguationDirs += newDir
    newDir
  }

  /**
    * Returns a unique local path for the given file/directory.
    * @param name the file/directory name
    * @param sourceContainer the source container of the file/directory (e.g. the
    *                        directory/folder it is coming from) - this must be
    *                        globally unique, to disambiguate between two identically
    *                        named folders in different file systems. All files from
    *                        from the same source container will be localized to the
    *                        same directory.
    * @param version an optional file version, in the case that the source file system
    *                uses versioning
    * @param defaultDir the directory where files should be placed unless
    *                  they name-collide with another file in the directory;
    *                  if None, each sourceContainer will map to a separate
    *                  local directory.
    * @param force whether to force the use of `defaultDir` - if true and defaultDir is
    *              specified, then the local path is always resolved to `defaultDir`. If
    *              there is a name collision, an exception is thrown rather than using a
    *              disambiguation directory.
    * @return the local path
    */
  private[util] def resolve(name: String,
                            sourceContainer: String,
                            version: Option[String] = None,
                            defaultDir: Option[Path] = None,
                            force: Boolean = false): Path = {
    logger.trace(s"getting local path for '${name}' from source container '${sourceContainer}''")
    val namePath = FileUtils.getPath(name)
    if (namePath.isAbsolute) {
      throw new Exception(s"expected ${name} to be a file name")
    }
    val localPath =
      if (defaultDir.isDefined && force) {
        val localPath = defaultDir.get.resolve(namePath)
        if (exists(localPath)) {
          throw new FileAlreadyExistsException(
              s"""Trying to localize ${name} from ${sourceContainer} to default directory 
                 |${defaultDir.get} but a file with that name already exists in that 
                 |directory and 'force' is true""".stripMargin
          )
        }
        localPath
      } else if (version.isDefined && sourceToTarget.contains((sourceContainer, version))) {
        // we have already seen this version - create the file in the existing dir
        val versionDir = sourceToTarget((sourceContainer, version))
        logger.trace(s"  version already seen; localizing to ${versionDir}")
        val versionLocalPath = versionDir.resolve(namePath)
        if (exists(versionLocalPath)) {
          throw new FileAlreadyExistsException(
              s"""Trying to localize ${name} (version ${version.get}) from ${sourceContainer}
                 |to ${versionDir} but the file already exists in that directory""".stripMargin
          )
        }
        versionLocalPath
      } else {
        // first try to place the file without using version; fall back to versioned
        // dir if version is defined
        sourceToTarget.get((sourceContainer, None)) match {
          case Some(parentDir) =>
            // if we already saw another file from the same source folder, try to
            // put this file in the same target directory
            logger.trace(s"  source folder already seen; trying to localize to ${parentDir}")
            val localPath = parentDir.resolve(namePath)
            if (!exists(localPath)) {
              // the local path doesn't exist yet - we can use it for this file/directory
              localPath
            } else if (version.isDefined) {
              // duplicate file - if there is a version, we place it in a subfolder named
              // after the version
              val versionDir = parentDir.resolve(FileUtils.sanitizeFileName(version.get))
              sourceToTarget += ((sourceContainer, version) -> versionDir)
              logger.trace(
                  s"""  target file ${localPath} already exists; trying to localize using version 
                    ${version.get} to ${versionDir}""".stripMargin.replaceAll("\n", " ")
              )
              val versionLocalPath = versionDir.resolve(namePath)
              if (exists(versionLocalPath)) {
                throw new FileAlreadyExistsException(
                    s"""Trying to localize ${name} (version ${version.get}) from ${sourceContainer}
                       |to ${versionDir} but the file already exists in that directory""".stripMargin
                )
              }
              versionLocalPath
            } else {
              throw new FileAlreadyExistsException(
                  s"""Trying to localize ${name} from ${sourceContainer} to ${parentDir}
                     |but the file already exists in that directory""".stripMargin
              )
            }
          case None =>
            // we have not seen the source container before - place the file in
            // the common dir if possible, otherwise a disambiguating subdir
            defaultDir.map(_.resolve(namePath)) match {
              case Some(localPath) if !exists(localPath) =>
                logger.trace(s"  localizing to default directory ${defaultDir}")
                sourceToTarget += ((sourceContainer, None) -> defaultDir.get)
                localPath
              case _ if canCreateDisambiguationDir =>
                // create a new disambiguation dir
                val newDir = createDisambiguationDir
                logger.trace(s"  localizing to new disambiguation directory ${newDir}")
                sourceToTarget += ((sourceContainer, None) -> newDir)
                newDir.resolve(namePath)
              case _ =>
                throw new Exception(
                    s"""|Trying to localize ${name} from ${sourceContainer} to local filesystem 
                        |at ${rootDir}/*/${name} and trying to create a new disambiguation dir, 
                        |but the limit (${disambiguationDirLimit}) has been reached.""".stripMargin
                      .replaceAll("\n", " ")
                )
            }
        }
      }
    logger.trace(s"  local path: ${localPath}")
    localizedPaths += localPath
    localPath
  }

  override def getLocalPathForSource(source: FileSource,
                                     sourceContainer: String,
                                     version: Option[String],
                                     localizationDir: Option[Path]): Path = {
    resolve(source.name, sourceContainer, version, localizationDir, force = true)
  }

  override def getLocalPath(source: AddressableFileSource,
                            localizationDir: Option[Path] = None): Path = {
    resolve(source.name, source.container, source.version, localizationDir, force = true)
  }

  override def getLocalPaths[T <: AddressableFileSource](
      fileSources: Iterable[T],
      localizationDir: Option[Path] = None
  ): Map[T, Path] = {
    if (separateDirsBySource) {
      fileSources.map(fs => fs -> getLocalPath(fs)).toMap
    } else {
      val (duplicates, singletons) = fileSources.groupBy(_.name).partition {
        case (_, sources) => sources.size > 1
      }
      val (separateSourceToPath, addToDefault) = if (duplicates.nonEmpty) {
        val duplicateFolders = duplicates.values.flatten.map(_.folder).toSet
        val (separate, default) = fileSources.partition(fs => duplicateFolders.contains(fs.folder))
        (separate.map { fs =>
          fs -> resolve(fs.name, fs.folder, defaultDir = localizationDir, force = true)
        }.toMap, default)
      } else {
        (Map.empty[T, Path], singletons.values.flatten)
      }
      val (defaultDir, force) = localizationDir match {
        case Some(dir) => (dir, true)
        case None      => (createDisambiguationDir, false)
      }
      val commonSourceToPath =
        addToDefault.map { fs =>
          fs -> resolve(fs.name, fs.container, fs.version, Some(defaultDir), force)
        }.toMap
      commonSourceToPath ++ separateSourceToPath
    }
  }
}

object SafeLocalizationDisambiguator {
  def create(rootDir: Path,
             existingPaths: Set[Path] = Set.empty,
             separateDirsBySource: Boolean = false,
             createDirs: Boolean = true,
             subdirPrefix: String = "input",
             disambiguationDirLimit: Int = 200,
             logger: Logger = Logger.get): SafeLocalizationDisambiguator = {
    SafeLocalizationDisambiguator(rootDir,
                                  separateDirsBySource,
                                  createDirs,
                                  subdirPrefix,
                                  disambiguationDirLimit,
                                  logger)(localizedPaths = existingPaths)
  }

  def fromJson(jsv: JsValue): SafeLocalizationDisambiguator = {
    jsv.asJsObject.getFields(
        "rootDir",
        "separateDirsBySource",
        "createDirs",
        "subdirPrefix",
        "disambiguationDirLimit",
        "logger",
        "sourceToTarget",
        "disambiguationDirs",
        "localizedPaths"
    ) match {
      case Seq(
          JsString(rootDir),
          JsBoolean(separateDirsBySource),
          JsBoolean(createDirs),
          JsString(subdirPrefix),
          JsNumber(disambiguationDirLimit),
          logger,
          JsArray(sourceToTarget),
          JsArray(disambiguationDirs),
          JsArray(localizedPaths)
          ) =>
        SafeLocalizationDisambiguator(Paths.get(rootDir),
                                      separateDirsBySource,
                                      createDirs,
                                      subdirPrefix,
                                      disambiguationDirLimit.toIntExact,
                                      logger.convertTo[Logger])(
            sourceToTarget.map {
              case JsObject(item) =>
                val container = JsUtils.getString(item, "container")
                val version = JsUtils.getOptionalString(item, "version")
                val path = JsUtils.getString(item, "path")
                (container, version) -> Paths.get(path)
              case other => throw new Exception(s"Invalid sourceToTarget item ${other}")
            }.toMap,
            disambiguationDirs.map {
              case JsString(path) => Paths.get(path)
              case other          => throw new Exception(s"Invalid disambiguation dir ${other}")

            }.toSet,
            localizedPaths.map {
              case JsString(path) => Paths.get(path)
              case other          => throw new Exception(s"Invalid localized path ${other}")
            }.toSet
        )
      case _ =>
        throw new Exception(s"Invalid serialized SafeLocalizationDisambiguator ${jsv}")
    }
  }
}
