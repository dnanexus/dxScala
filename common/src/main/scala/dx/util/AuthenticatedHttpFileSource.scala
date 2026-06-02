package dx.util

import java.io.{ByteArrayOutputStream, FileOutputStream, OutputStream}
import java.net.{HttpURLConnection, URI}
import java.nio.charset.Charset
import java.nio.file.{Files, Path}

sealed trait HttpAuthenticationScheme {
  def value: String
}

object HttpAuthenticationScheme {
  case object Bearer extends HttpAuthenticationScheme { val value = "Bearer" }

  def fromString(s: String): Option[HttpAuthenticationScheme] = s.toLowerCase match {
    case "bearer" => Some(Bearer)
    case _        => None
  }
}

case class HttpCredentials(scheme: HttpAuthenticationScheme, credentials: String)

/**
  * An HTTP-backed FileSource that attaches an `Authorization` header on every
  * request when credentials are provided, and surfaces 401/403 responses as
  * exceptions with actionable guidance.
  *
  * `tokenEnvVarHint`, when provided, is surfaced verbatim in the 401 error
  * message so users know which environment variable to set. The library is
  * intentionally agnostic about the name; callers supply it.
  */
case class AuthenticatedHttpFileSource(
    override val uri: URI,
    override val encoding: Charset,
    override val isDirectory: Boolean,
    credentials: Option[HttpCredentials] = None,
    tokenEnvVarHint: Option[String] = None,
    logger: Logger = Logger.Quiet
)(override val address: String)
    extends AbstractAddressableFileNode(address, encoding) {

  private lazy val path = PosixPath(uri.getPath)

  override lazy val name: String =
    path.getName.getOrElse(throw new Exception(s"${path} is not a file"))

  override lazy val folder: String = path.getParent.map(_.toString).getOrElse("")

  override def container: String = s"${uri.getScheme}:${uri.getHost}:${folder}"

  private var hasBytes: Boolean = false

  private def withConnection[T](fn: HttpURLConnection => T): T = {
    val url = uri.toURL
    var conn: HttpURLConnection = null
    try {
      conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("HEAD")
      credentials.foreach { c =>
        conn.setRequestProperty("Authorization", s"${c.scheme.value} ${c.credentials}")
      }
      fn(conn)
    } finally {
      if (conn != null) {
        conn.disconnect()
      }
    }
  }

  private def unauthorizedHint: String = tokenEnvVarHint match {
    case Some(envVar) =>
      s"""Bearer tokens can be supplied via the ${envVar} environment variable. The value must be in the format domain:token[;domain:token]*, for example: raw.githubusercontent.com:<YOUR_GITHUB_TOKEN>;example.com:<YOUR_GITLAB_TOKEN>."""
    case None =>
      "Supply Bearer credentials to access this resource."
  }

  private def throwOnWrongAuth(responseCode: Int): Unit = {
    responseCode match {
      case HttpURLConnection.HTTP_UNAUTHORIZED =>
        throw new Exception(
            s"""HTTP 401 Unauthorized when accessing ${uri}.
               |If this is a private repository, ensure the credentials are provided. Currently supported authentication types: Bearer token.
               |${unauthorizedHint}""".stripMargin
        )
      case HttpURLConnection.HTTP_FORBIDDEN =>
        throw new Exception(
            s"""HTTP 403 Forbidden when accessing ${uri}.
               |If this is a private repository, this may indicate that the provided credentials are invalid or lack the required permissions.
               |For example, a GitHub token must have 'repo' scope to access private repositories.""".stripMargin
        )
      case _ => ()
    }
  }

  override def exists: Boolean = {
    try {
      val rc = withConnection(conn => conn.getResponseCode)
      rc match {
        case HttpURLConnection.HTTP_OK => true
        case _ =>
          throwOnWrongAuth(rc)
          false
      }
    } catch {
      case _: java.net.UnknownHostException => false
      case e: Exception                     => throw e
    }
  }

  override def getParent: Option[AuthenticatedHttpFileSource] = {
    if (path.getParent == null) {
      None
    } else {
      val newUri = if (isDirectory) {
        uri.resolve("..")
      } else {
        uri.resolve(".")
      }
      Some(
          AuthenticatedHttpFileSource(newUri,
                                      encoding,
                                      isDirectory = true,
                                      credentials,
                                      tokenEnvVarHint,
                                      logger)(
              newUri.toString
          )
      )
    }
  }

  private def resolve(path: String, isDir: Boolean): AuthenticatedHttpFileSource = {
    val newUri = if (isDirectory) {
      uri.resolve(path)
    } else {
      uri.resolve(".").resolve(path)
    }
    AuthenticatedHttpFileSource(newUri, encoding, isDir, credentials, tokenEnvVarHint, logger)(
        newUri.toString
    )
  }

  override def resolve(path: String): AuthenticatedHttpFileSource = {
    resolve(path, isDir = false)
  }

  override def resolveDirectory(path: String): AuthenticatedHttpFileSource = {
    resolve(path, isDir = true)
  }

  override def relativize(fileSource: AddressableFileSource): String = {
    fileSource match {
      case fs: AuthenticatedHttpFileSource if isDirectory =>
        path.relativize(fs.path).toString
      case fs: AuthenticatedHttpFileSource =>
        path.getParent.get.relativize(fs.path).toString
      case _ =>
        throw new Exception(s"not an AuthenticatedHttpFileSource: ${fileSource}")
    }
  }

  override lazy val size: Long = {
    try {
      withConnection(conn => {
        conn.setRequestMethod("HEAD")
        conn.getContentLengthLong
      })
    } catch {
      case t: Throwable =>
        throw new Exception(s"Error getting size of URL ${uri}: ${t.getMessage}")
    }
  }

  private def fetchUri(buffer: OutputStream, chunkSize: Int = 16384): Int = {
    withConnection { conn =>
      conn.setRequestMethod("GET")
      val responseCode = conn.getResponseCode
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throwOnWrongAuth(responseCode)
        throw new Exception(s"Error fetching URL ${uri}: HTTP ${responseCode}")
      }
      val is = conn.getInputStream
      try {
        var nRead = 0
        var totalRead = 0
        val data = new Array[Byte](chunkSize)
        do {
          nRead = is.read(data, 0, chunkSize)
          if (nRead > 0) {
            buffer.write(data, 0, nRead)
            totalRead += nRead
          }
        } while (nRead > 0)
        totalRead
      } finally {
        is.close()
      }
    }
  }

  override lazy val readBytes: Array[Byte] = {
    checkFileSize()
    val buffer = new ByteArrayOutputStream()
    try {
      fetchUri(buffer)
      hasBytes = true
      buffer.toByteArray
    } finally {
      buffer.close()
    }
  }

  private def localizeToFile(path: Path): Unit = {
    if (hasBytes) {
      FileUtils.writeFileContent(path, new String(readBytes, encoding))
    } else {
      val buffer = new FileOutputStream(path.toFile)
      try {
        fetchUri(buffer)
      } finally {
        buffer.close()
      }
    }
  }

  override protected def localizeTo(file: Path): Unit = {
    if (isDirectory) {
      val dest = Files.createTempFile("temp", name)
      try {
        localizeToFile(dest)
        if (Files.exists(file)) {
          FileUtils.deleteRecursive(file)
        }
        FileUtils.unpackArchive(dest, file)
      } finally {
        FileUtils.deleteRecursive(dest)
      }
    } else {
      localizeToFile(file)
    }
  }

  override def isListable: Boolean = false
}

/**
  * A FileAccessProtocol for http/https URLs that attaches Bearer credentials
  * based on a per-domain token map.
  *
  * `tokenEnvVarHint` is forwarded to constructed `AuthenticatedHttpFileSource`
  * instances so 401 error messages can guide the user to the env var the
  * caller exposes. The library does not own that env var name.
  */
case class AuthenticatedHttpFileAccessProtocol(
    encoding: Charset = FileUtils.DefaultEncoding,
    domainBearerTokens: Map[String, String] = Map.empty,
    tokenEnvVarHint: Option[String] = None,
    logger: Logger = Logger.Quiet
) extends FileAccessProtocol {
  override val schemes = Vector(FileUtils.HttpScheme, FileUtils.HttpsScheme)
  override val supportsDirectories: Boolean = true

  private def domainBearerTokenForUri(uri: URI): Option[String] = {
    Option(uri.getHost).flatMap { host =>
      domainBearerTokens.collectFirst {
        case (domain, token) if domain.equalsIgnoreCase(host) => token
      }
    }
  }

  private def credentialsForUri(uri: URI): Option[HttpCredentials] = {
    domainBearerTokenForUri(uri).map { token =>
      logger.trace(s"Using Bearer token authenticated HTTP for import from: ${uri.getHost}")
      HttpCredentials(HttpAuthenticationScheme.Bearer, token)
    }
  }

  def resolve(uri: URI, value: Option[String] = None): AuthenticatedHttpFileSource = {
    AuthenticatedHttpFileSource(uri,
                                encoding,
                                isDirectory = false,
                                credentialsForUri(uri),
                                tokenEnvVarHint,
                                logger)(value.getOrElse(uri.toString))
  }

  override def resolve(address: String): AuthenticatedHttpFileSource = {
    resolve(URI.create(address), Some(address))
  }

  override def resolveDirectory(address: String): AuthenticatedHttpFileSource = {
    val uri = URI.create(address)
    AuthenticatedHttpFileSource(uri,
                                encoding,
                                isDirectory = true,
                                credentialsForUri(uri),
                                tokenEnvVarHint,
                                logger)(address)
  }
}

object AuthenticatedHttpFileAccessProtocol {

  /**
    * Parses a string of the form `domain:token[;domain:token]*` into a
    * domain -> token map. Splits each entry on the first colon only, so
    * tokens containing colons are supported. Domains are lowercased; tokens
    * preserve case.
    */
  def parseTokens(value: String): Map[String, String] = {
    value
      .split(";")
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap { entry =>
        val idx = entry.indexOf(':')
        if (idx > 0 && idx < entry.length - 1) {
          val domain = entry.substring(0, idx).trim.toLowerCase
          val token = entry.substring(idx + 1).trim
          if (domain.nonEmpty && token.nonEmpty) Some(domain -> token) else None
        } else {
          None
        }
      }
      .toMap
  }
}
