package dx.util

import java.nio.file.Files

import spray.json._

import scala.util.{Success, Try}

case class DockerUtils(fileResolver: FileSourceResolver = FileSourceResolver.get,
                       logger: Logger = Logger.get) {
  private lazy val DOCKER_TARBALLS_DIR = {
    val p = Files.createTempDirectory("docker-tarballs")
    sys.addShutdownHook(FileUtils.deleteRecursive(p))
    p
  }

//  private lazy val dockerVersion: Vector[Int] = {
//    val dockerVersionRegexp = "(?s)Docker version (\\d+)\\.(\\d+)\\.(\\d+).*".r
//    SysUtils.execCommand("docker --version") match {
//      case (0, dockerVersionRegexp(major, minor, patch), _) =>
//        Vector(major.toInt, minor.toInt, patch.toInt)
//      case other =>
//        throw new Exception(s"unexpected docker --version output ${other}")
//    }
//  }
//
//  private def dockerVersionCompare(ver: Vector[Int]): Int = {
//    dockerVersion
//      .zip(ver)
//      .collectFirst {
//        case (a, b) if a < b => -1
//        case (a, b) if a > b => 1
//      }
//      .getOrElse(0)
//  }

  private val dockerPullStdoutRegexp = "(?s).*[\n ](.+)".r

  // pull a Docker image from a repository - requires Docker client to be installed
  def pullImage(name: String, maxRetries: Int = 3): String = {
    def pull(retry: Int): Option[String] = {
      try {
        // the last line of stdout ends with the full image name
        SysUtils.execCommand(s"docker pull ${name}") match {
          case (0, dockerPullStdoutRegexp(imageName), _) => Some(imageName.trim)
          case other =>
            throw new Exception(s"unexpected output from 'docker pull': ${other}")
        }
      } catch {
        case t: CommandExecError =>
          logger.trace(
              s"Failed to pull docker image: ${name}. Retrying... ${maxRetries - retry}",
              exception = Some(t)
          )
          Thread.sleep(1000)
          None
      }
    }

    (0 to maxRetries)
      .collectFirst { retry =>
        pull(retry) match {
          case Some(name) => name
        }
      }
      .getOrElse(
          throw new Exception(s"Unable to pull docker image: ${name} after ${maxRetries} tries")
      )
  }

  // Read the manifest file from a docker tarball, and get the repository name.
  //
  // A manifest could look like this:
  // [
  //    {"Config":"4b778ee055da936b387080ba034c05a8fad46d8e50ee24f27dcd0d5166c56819.json",
  //     "RepoTags":["ubuntu_18_04_minimal:latest"],
  //     "Layers":[
  //          "1053541ae4c67d0daa87babb7fe26bf2f5a3b29d03f4af94e9c3cb96128116f5/layer.tar",
  //          "fb1542f1963e61a22f9416077bf5f999753cbf363234bf8c9c5c1992d9a0b97d/layer.tar",
  //          "2652f5844803bcf8615bec64abd20959c023d34644104245b905bb9b08667c8d/layer.tar",
  //          ]}
  // ]
  private[util] def readManifestGetDockerImageName(buf: String): Option[String] = {
    val jso = buf.parseJson
    val elem = jso match {
      case JsArray(elements) if elements.nonEmpty => elements.head
      case other =>
        logger.warning(s"bad value ${other} for manifest, expecting non empty array")
        return None
    }
    elem.asJsObject.fields.get("RepoTags") match {
      case None | Some(JsNull) =>
        logger.warning("The repository is not specified for the image")
        None
      case Some(JsString(repo)) =>
        Some(repo)
      case Some(JsArray(elements)) if elements.isEmpty =>
        logger.warning("RepoTags has an empty array")
        None
      case Some(JsArray(elements)) =>
        elements.head match {
          case JsString(repo) => Some(repo)
          case other =>
            logger.warning(s"bad value ${other} in RepoTags manifest field")
            None
        }
      case other =>
        logger.warning(s"bad value ${other} in RepoTags manifest field")
        None
    }
  }

  private val imageRegexp = "(?:(.+)://)?(.+)".r
  private val dockerRepoRegexp = "^Loaded image: (.+)$".r
  private val dockerHashRegexp = "^Loaded image ID: (.+)$".r

  // If `nameOrUri` is a URI, the Docker image tarball is downloaded using the fileResovler,
  // and loaded using `docker load`. The image name is preferentially taken from the tar
  // manifest, but the output of `docker load` is used as a fallback. Otherwise, it is assumed
  // to be an image name and is pulled with `pullImage`. Requires Docker client to be installed.
  // TODO: I'm not sure that the manifest should take priority over the output of 'docker load'
  // TODO: handle single-layer images with `docker import` instead of `docker load`
  def getImage(nameOrUri: String): String = {
    val (protocol, name) = nameOrUri match {
      case imageRegexp(null, name)     => (None, name)
      case imageRegexp(protocol, name) => (Some(protocol), name)
      case _ =>
        throw new Exception(s"invalid image name or URL ${nameOrUri}")
    }
    if (protocol.exists(fileResolver.canResolve)) {
      // a tarball created with "docker save".
      // 1. download it
      // 2. load into the local docker cache
      // 3. figure out the image name from either the manifest or the output of `docker load`
      logger.traceLimited(s"downloading docker tarball ${nameOrUri} to ${DOCKER_TARBALLS_DIR}")
      val localTarSrc =
        try {
          fileResolver.resolve(nameOrUri)
        } catch {
          case e: NoSuchProtocolException =>
            throw new Exception(s"Could not resolve docker image URI ${nameOrUri}", e)
        }
      val localTar = localTarSrc.localizeToDir(DOCKER_TARBALLS_DIR, overwrite = true)
      logger.traceLimited(s"load tarball ${localTar} to docker", minLevel = TraceLevel.None)
      val (_, outstr, errstr) = SysUtils.execCommand(s"docker load --input ${localTar}")
      logger.traceLimited(
          s"""|output:
              |${outstr}
              |stderr:
              |${errstr}""".stripMargin
      )
      val repo = Try {
        logger.trace("trying to determine image name from tarball manifest")
        val (_, mContent, _) =
          SysUtils.execCommand(s"tar --to-stdout -xf ${localTar} manifest.json")
        logger.traceLimited(
            s"""|manifest content:
                |${mContent}
                |""".stripMargin
        )
        readManifestGetDockerImageName(mContent)
      }.toOption.flatten.getOrElse {
        logger.trace(
            "unable to determine image name from tarball manifest; using docker load output instead"
        )
        outstr.trim match {
          case dockerRepoRegexp(r) => r
          case dockerHashRegexp(h) => h
          case _ =>
            throw new Exception(
                s"Could not determine the repo name from either the manifest or the 'docker load' output"
            )
        }
      }
      logger.traceLimited(s"repository is ${repo}")
      repo
    } else if (protocol.forall(_ == "docker")) {
      // the protocol is 'docker' or there is no protocol ('docker' by default)
      pullImage(name)
    } else {
      throw new Exception(
          s"""Only Docker images in a repository or created via 'docker save' are 
             |currently supported; cannot pull ${nameOrUri}""".stripMargin.replaceAll("\n", " ")
      )
    }
  }

  def getImage(nameOrUrlVec: Vector[String]): String = {
    nameOrUrlVec
      .collectFirst { nameOrUrl =>
        Try(getImage(nameOrUrl)) match {
          case Success(value) => value
        }
      }
      .getOrElse(
          throw new Exception(s"Could not get image from any of ${nameOrUrlVec}")
      )
  }
}
