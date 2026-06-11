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

case class HttpCredentials(scheme: HttpAuthenticationScheme, credentials: String) {
  override def toString: String = s"HttpCredentials(${scheme.value}, ***)"
}

/**
  * An HTTP-backed FileSource that attaches an `Authorization` header on every
  * request when credentials are provided, and surfaces 401/403 responses as
  * exceptions with actionable guidance.
  *
  * `unauthorizedHint`, when provided, is appended verbatim to the 401 error
  * message so callers can point users at whatever credential mechanism their
  * application exposes. Defaults to `AuthenticatedHttpFileSource.DefaultHint`
  * when omitted.
  */
case class AuthenticatedHttpFileSource(
    override val uri: URI,
    override val encoding: Charset,
    override val isDirectory: Boolean,
    credentials: Option[HttpCredentials] = None,
    unauthorizedHint: Option[String] = None,
    logger: Logger = Logger.Quiet
)(override val address: String)
    extends AbstractAddressableFileNode(address, encoding) {

  private lazy val path = PosixPath(uri.getPath)

  override lazy val name: String =
    path.getName.getOrElse(throw new Exception(s"${path} is not a file"))

  override lazy val folder: String = path.getParent.map(_.toString).getOrElse("")

  override def container: String = s"${uri.getScheme}:${uri.getHost}:${folder}"

  private var hasBytes: Boolean = false

  private def openConnection(method: String): HttpURLConnection = {
    val conn = uri.toURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    credentials.foreach { c =>
      conn.setRequestProperty("Authorization", s"${c.scheme.value} ${c.credentials}")
    }
    conn
  }

  private def withConnection[T](method: String = "HEAD")(fn: HttpURLConnection => T): T = {
    var conn: HttpURLConnection = null
    try {
      conn = openConnection(method)
      // Many servers reject HEAD on a resource that GET would serve (RFC 7231
      // §6.5.5). Transparently retry such requests with GET so callers can rely
      // on HEAD-style "exists / size" probes regardless of server quirks.
      if (method == "HEAD" && conn.getResponseCode == HttpURLConnection.HTTP_BAD_METHOD) {
        conn.disconnect()
        conn = openConnection("GET")
      }
      fn(conn)
    } finally {
      if (conn != null) {
        conn.disconnect()
      }
    }
  }

  private def throwOnWrongAuth(responseCode: Int): Unit = {
    val hint = unauthorizedHint.getOrElse(AuthenticatedHttpFileSource.DefaultHint)
    responseCode match {
      case HttpURLConnection.HTTP_UNAUTHORIZED =>
        throw new Exception(
            s"""HTTP 401 Unauthorized when accessing ${uri}.
               |If this is a private repository, ensure the credentials are provided. Currently supported authentication types: Bearer token.
               |${hint}""".stripMargin
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
      val rc = withConnection()(conn => conn.getResponseCode)
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
    path.getParent.map { _ =>
      val newUri = if (isDirectory) {
        uri.resolve("..")
      } else {
        uri.resolve(".")
      }
      AuthenticatedHttpFileSource(newUri,
                                  encoding,
                                  isDirectory = true,
                                  credentials,
                                  unauthorizedHint,
                                  logger)(newUri.toString)
    }
  }

  private def resolve(path: String, isDir: Boolean): AuthenticatedHttpFileSource = {
    val newUri = if (isDirectory) {
      uri.resolve(path)
    } else {
      uri.resolve(".").resolve(path)
    }
    AuthenticatedHttpFileSource(newUri, encoding, isDir, credentials, unauthorizedHint, logger)(
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
        path.getParent
          .getOrElse(throw new Exception(s"Cannot relativize: ${address} has no parent directory"))
          .relativize(fs.path)
          .toString
      case _ =>
        throw new Exception(s"not an AuthenticatedHttpFileSource: ${fileSource}")
    }
  }

  override lazy val size: Long = {
    withConnection() { conn =>
      val responseCode = conn.getResponseCode
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throwOnWrongAuth(responseCode)
        throw new Exception(s"Error getting size of URL ${uri}: HTTP ${responseCode}")
      }
      conn.getContentLengthLong
    }
  }

  private def fetchUri(buffer: OutputStream, chunkSize: Int = 16384): Int = {
    withConnection("GET") { conn =>
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
    Option(path.getParent).foreach(FileUtils.createDirectories)
    if (hasBytes) {
      Files.write(path, readBytes)
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

object AuthenticatedHttpFileSource {

  /** Generic fallback hint used when no `unauthorizedHint` is supplied. */
  val DefaultHint: String = "Supply Bearer credentials to access this resource."
}

/**
  * A FileAccessProtocol for http/https URLs that attaches Bearer credentials
  * based on a per-domain token map.
  *
  * `unauthorizedHint` is forwarded to every constructed
  * `AuthenticatedHttpFileSource` so 401 error messages can guide the user to
  * whatever credential mechanism the caller exposes.
  */
case class AuthenticatedHttpFileAccessProtocol(
    encoding: Charset = FileUtils.DefaultEncoding,
    domainBearerTokens: Map[String, String] = Map.empty,
    unauthorizedHint: Option[String] = None,
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
                                unauthorizedHint,
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
                                unauthorizedHint,
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
  def parseAuthTokens(value: String): Map[String, String] = {
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
