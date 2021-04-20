package dx.util

import dx.util.CollectionUtils.IterableOnceExtensions
import java.nio.file.{FileAlreadyExistsException, Files, Path}
import java.util.UUID

trait LocalizationDisambiguator {

  /**
    * Returns a unique local path for a single source file.
    */
  def getLocalPath(fileSource: AddressableFileSource): Path

  /**
    * Returns a mapping of file source to unique local path.
    * This method is only meant to be called a single time for a given instance.
    */
  def getLocalPaths(fileSources: Vector[AddressableFileSource]): Map[AddressableFileSource, Path]
}

/**
  * Localizes a file according to the rules in the spec:
  * https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#task-input-localization.
  * - two input files with the same name must be located separately, to avoid name collision
  * - two input files that originated in the same storage directory must also be localized into
  *   the same directory for task execution
  * @param rootDir the root dir - files are localize to subdirectories under this directory
  * @param existingPaths optional Set of paths that should be assumed to already exist locally
  * @param separateDirsBySource whether to always separate files from each source dir into
  *                             separate target dirs (true), or to minimize the number of
  *                              dirs used by putting all files in a single directory by default
  *                              but create additional directories to avoid name collision.
  * @param createDirs whether to create the directories - set to true unless you are just using
  *                   this class to create unique names for files that will be synced e.g. using
  *                   a FUSE filesystem.
  * @param subdirPrefix prefix to add to localization dirs
  * @param disambiguationDirLimit max number of disambiguation subdirs that can be created
  */
case class SafeLocalizationDisambiguator(
    rootDir: Path,
    existingPaths: Set[Path] = Set.empty,
    separateDirsBySource: Boolean = false,
    createDirs: Boolean = true,
    subdirPrefix: String = "input",
    disambiguationDirLimit: Int = 200
) extends LocalizationDisambiguator {
  // mapping from source file parent directories to local directories - this
  // ensures that files that were originally from the same directory are
  // localized to the same target directory
  private var sourceToTarget: Map[String, Path] = Map.empty
  // keep track of which disambiguation dirs we've created
  private var disambiguationDirs: Set[Path] = Set.empty
  // keep track of which Paths we've returned so we can detect collisions
  private var localizedPaths: Set[Path] = existingPaths

  def getLocalizedPaths: Set[Path] = localizedPaths

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
      // we should never get a collision according to the guarantees of '
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

  private def getLocalPath(name: String,
                           sourceFolder: String,
                           commonDir: Option[Path] = None): Path = {
    val localPath = sourceToTarget.get(sourceFolder) match {
      case Some(parentDir) =>
        // if we already saw another file from the same source folder as `source`, try to
        // put `source` in that same target directory
        val localPath = parentDir.resolve(name)
        if (exists(localPath)) {
          throw new FileAlreadyExistsException(
              s"""Trying to localize ${name} from ${sourceFolder} to ${parentDir}
                 |but the file already exists in that directory""".stripMargin
          )
        }
        localPath
      case None =>
        commonDir.map(_.resolve(name)) match {
          case Some(localPath) if !exists(localPath) =>
            sourceToTarget += (sourceFolder -> commonDir.get)
            localPath
          case _ if canCreateDisambiguationDir =>
            // create a new disambiguation dir
            val newDir = createDisambiguationDir
            sourceToTarget += (sourceFolder -> newDir)
            newDir.resolve(name)
          case _ =>
            throw new Exception(
                s"""|Trying to localize ${name} from ${sourceFolder} to local filesystem 
                    |at ${rootDir}/*/${name} and trying to create a new disambiguation dir, 
                    |but the limit (${disambiguationDirLimit}) has been reached.""".stripMargin
                  .replaceAll("\n", " ")
            )
        }
    }
    localizedPaths += localPath
    localPath
  }

  /**
    * Because we have to keep together files from the same source folder,
    * and there's no way to know in advance if there will be another file
    * from the same source folder as this file and whether it will have a
    * name collision, a separate disambiguation dir is created for each
    * source folder regardless of the value of `separateDirsBySource`.
    */
  override def getLocalPath(source: AddressableFileSource): Path = {
    getLocalPath(source.name, source.folder)
  }

  /**
    * If `separateDirsBySource` is `false`, then we use a common disambiguation
    * directory for all files unless there is a naming collision, in which case
    * the file with a collision is placed in a separate dir along with any other
    * files that came from the same source.
    */
  override def getLocalPaths(
      fileSources: Vector[AddressableFileSource]
  ): Map[AddressableFileSource, Path] = {
    if (separateDirsBySource) {
      fileSources.map(fs => fs -> getLocalPath(fs)).toMap
    } else {
      val (duplicates, singletons) = fileSources.groupBy(_.name).partition {
        case (_, sources) => sources.size > 1
      }
      val (separateSourceToPath, addToCommon) = if (duplicates.nonEmpty) {
        val duplicateFolders = duplicates.values.flatten.map(_.folder).toSet
        val (separate, common) = fileSources.partition(fs => duplicateFolders.contains(fs.folder))
        (separate.map(fs => fs -> getLocalPath(fs.name, fs.folder)).toMap, common)
      } else {
        (Map.empty[AddressableFileSource, Path], singletons.values.flatten)
      }
      val commonDir = Some(createDisambiguationDir)
      val commonSourceToPath =
        addToCommon.map { fs =>
          fs -> getLocalPath(fs.name, fs.folder, commonDir)
        }.toMap
      commonSourceToPath ++ separateSourceToPath
    }
  }
}
