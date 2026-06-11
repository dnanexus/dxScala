package dx.util

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

class AuthenticatedHttpFileSourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Records the Authorization header value seen on each request, in order.
  private val authHeadersSeen = new ConcurrentLinkedQueue[Option[String]]()

  private def respond(exchange: HttpExchange, status: Int, body: Array[Byte]): Unit = {
    authHeadersSeen.add(Option(exchange.getRequestHeaders.getFirst("Authorization")))
    if (exchange.getRequestMethod == "HEAD") {
      // HEAD must advertise Content-Length without writing a body. Setting
      // the header before sendResponseHeaders(_, -1) is the JDK-supported way.
      exchange.getResponseHeaders.set("Content-Length", body.length.toString)
      exchange.sendResponseHeaders(status, -1)
      exchange.close()
    } else {
      exchange.sendResponseHeaders(status, body.length.toLong)
      val os = exchange.getResponseBody
      try os.write(body)
      finally os.close()
    }
  }

  private val okBody = "hello".getBytes(StandardCharsets.UTF_8)

  private val handler200: HttpHandler = (exchange: HttpExchange) => respond(exchange, 200, okBody)
  private val handler401: HttpHandler = (exchange: HttpExchange) =>
    respond(exchange, 401, Array.emptyByteArray)
  private val handler403: HttpHandler = (exchange: HttpExchange) =>
    respond(exchange, 403, Array.emptyByteArray)
  // Returns 200 only when a matching bearer token is presented, else 401.
  private val expectedToken = "secret-token-123"
  private val handlerBearerProtected: HttpHandler = (exchange: HttpExchange) => {
    val header = Option(exchange.getRequestHeaders.getFirst("Authorization"))
    if (header.contains(s"Bearer $expectedToken")) respond(exchange, 200, okBody)
    else respond(exchange, 401, Array.emptyByteArray)
  }
  // Responds 200 OK without a Content-Length header — simulates servers using
  // chunked transfer encoding (e.g. GitHub raw URLs).
  private val handlerNoLength: HttpHandler = (exchange: HttpExchange) => {
    if (exchange.getRequestMethod == "HEAD") {
      exchange.sendResponseHeaders(200, -1)
      exchange.close()
    } else {
      // responseLength=0 -> chunked transfer encoding, no Content-Length sent
      exchange.sendResponseHeaders(200, 0)
      val os = exchange.getResponseBody
      try os.write(okBody)
      finally os.close()
    }
  }

  private var server: HttpServer = _
  private var baseUri: URI = _

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/ok", handler200)
    server.createContext("/unauthorized", handler401)
    server.createContext("/forbidden", handler403)
    server.createContext("/protected", handlerBearerProtected)
    server.createContext("/binary", (exchange: HttpExchange) => respond(exchange, 200, binaryBody))
    server.createContext("/chunked", handlerNoLength)
    server.setExecutor(null)
    server.start()
    val port = server.getAddress.getPort
    baseUri = URI.create(s"http://127.0.0.1:$port")
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
  }

  private def fs(path: String,
                 credentials: Option[HttpCredentials] = None,
                 unauthorizedHint: Option[String] = None): AuthenticatedHttpFileSource = {
    val uri = baseUri.resolve(path)
    AuthenticatedHttpFileSource(uri,
                                StandardCharsets.UTF_8,
                                isDirectory = false,
                                credentials,
                                unauthorizedHint)(uri.toString)
  }

  private def bearer(token: String): HttpCredentials =
    HttpCredentials(HttpAuthenticationScheme.Bearer, token)

  private def lastAuthHeader(): Option[String] = {
    authHeadersSeen.asScala.lastOption.flatten
  }

  // --- HttpAuthenticationScheme -----------------------------------------

  it should "expose the Bearer scheme with the correct header value" in {
    HttpAuthenticationScheme.Bearer.value shouldBe "Bearer"
  }

  it should "redact the credentials value in HttpCredentials.toString" in {
    val credentials = HttpCredentials(HttpAuthenticationScheme.Bearer, "super-secret-token")
    val rendered = credentials.toString
    rendered shouldBe "HttpCredentials(Bearer, ***)"
    rendered should not include "super-secret-token"
  }

  it should "parse the Bearer scheme name case-insensitively via fromString" in {
    HttpAuthenticationScheme.fromString("bearer") shouldBe Some(HttpAuthenticationScheme.Bearer)
    HttpAuthenticationScheme.fromString("BEARER") shouldBe Some(HttpAuthenticationScheme.Bearer)
    HttpAuthenticationScheme.fromString("Bearer") shouldBe Some(HttpAuthenticationScheme.Bearer)
  }

  it should "return None from fromString for unknown schemes" in {
    HttpAuthenticationScheme.fromString("basic") shouldBe None
    HttpAuthenticationScheme.fromString("digest") shouldBe None
    HttpAuthenticationScheme.fromString("") shouldBe None
  }

  // --- Without credentials ----------------------------------------------

  it should "return true from exists when the server responds 200" in {
    authHeadersSeen.clear()
    fs("/ok/file.txt").exists shouldBe true
    lastAuthHeader() shouldBe None
  }

  it should "throw from exists on 401 even when no credentials are configured" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt").exists
    thrown.getMessage should include("HTTP 401 Unauthorized")
  }

  it should "throw from exists on 403 even when no credentials are configured" in {
    val thrown = the[Exception] thrownBy fs("/forbidden/file.txt").exists
    thrown.getMessage should include("HTTP 403 Forbidden")
  }

  it should "return false from exists when the host cannot be resolved" in {
    val uri = URI.create("http://no-such-host.invalid./missing.txt")
    AuthenticatedHttpFileSource(uri, StandardCharsets.UTF_8, isDirectory = false, None)(
        uri.toString
    ).exists shouldBe false
  }

  it should "read bytes from a 200 response when no credentials are configured" in {
    new String(fs("/ok/file.txt").readBytes, StandardCharsets.UTF_8) shouldBe "hello"
  }

  it should "throw from readBytes on a 401 response when no credentials are configured" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt").readBytes
    thrown.getMessage should include("HTTP 401 Unauthorized")
  }

  // --- With Bearer credentials ------------------------------------------

  it should "send an 'Authorization: Bearer <token>' header when Bearer credentials are configured" in {
    authHeadersSeen.clear()
    fs("/ok/file.txt", Some(bearer("abc"))).exists shouldBe true
    lastAuthHeader() shouldBe Some("Bearer abc")
  }

  it should "succeed against a bearer-protected endpoint when the token matches" in {
    fs("/protected/file.txt", Some(bearer(expectedToken))).exists shouldBe true
  }

  it should "throw from exists with the 401 guidance message when credentials are configured but rejected" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt", Some(bearer("bad"))).exists
    thrown.getMessage should include("HTTP 401 Unauthorized")
    thrown.getMessage should include("Bearer token")
  }

  it should "throw from exists with the 403 guidance message when credentials are configured but forbidden" in {
    val thrown = the[Exception] thrownBy fs("/forbidden/file.txt", Some(bearer("bad"))).exists
    thrown.getMessage should include("HTTP 403 Forbidden")
    thrown.getMessage should include("'repo' scope")
  }

  it should "throw from readBytes with the 401 guidance message when credentials are configured but rejected" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt", Some(bearer("bad"))).readBytes
    thrown.getMessage should include("HTTP 401 Unauthorized")
  }

  // --- unauthorizedHint propagation in the 401 message -------------------

  it should "include the supplied unauthorizedHint verbatim in the 401 guidance message" in {
    val hint = "Set MY_BEARER_TOKENS_VAR=domain:token to authenticate."
    val thrown = the[Exception] thrownBy fs(
        "/unauthorized/file.txt",
        Some(bearer("bad")),
        unauthorizedHint = Some(hint)
    ).exists
    thrown.getMessage should include("HTTP 401 Unauthorized")
    thrown.getMessage should include(hint)
  }

  it should "fall back to AuthenticatedHttpFileSource.DefaultHint in the 401 message when no hint is supplied" in {
    val thrown = the[Exception] thrownBy fs(
        "/unauthorized/file.txt",
        Some(bearer("bad"))
    ).exists
    thrown.getMessage should include("HTTP 401 Unauthorized")
    thrown.getMessage should include(AuthenticatedHttpFileSource.DefaultHint)
  }

  it should "propagate unauthorizedHint through resolve" in {
    val parent = fs("/ok/", Some(bearer("tok")), unauthorizedHint = Some("X"))
    parent.resolve("child.txt").unauthorizedHint shouldBe Some("X")
  }

  it should "propagate unauthorizedHint through resolveDirectory" in {
    val parent = fs("/ok/", Some(bearer("tok")), unauthorizedHint = Some("X"))
    parent.resolveDirectory("sub").unauthorizedHint shouldBe Some("X")
  }

  it should "propagate unauthorizedHint through getParent" in {
    val child = fs("/ok/dir/file.txt", Some(bearer("tok")), unauthorizedHint = Some("X"))
    child.getParent.flatMap(_.unauthorizedHint) shouldBe Some("X")
  }

  // --- credentials propagation -----------------------------------------

  it should "propagate credentials through resolve" in {
    val parent = fs("/ok/", Some(bearer("tok")))
    parent.resolve("child.txt").credentials shouldBe Some(bearer("tok"))
  }

  it should "propagate credentials through resolveDirectory" in {
    val parent = fs("/ok/", Some(bearer("tok")))
    parent.resolveDirectory("sub").credentials shouldBe Some(bearer("tok"))
  }

  it should "propagate credentials through getParent" in {
    val child = fs("/ok/dir/file.txt", Some(bearer("tok")))
    child.getParent.flatMap(_.credentials) shouldBe Some(bearer("tok"))
  }

  // --- size -------------------------------------------------------------

  it should "return the Content-Length from size on 200" in {
    fs("/ok/file.txt").size shouldBe okBody.length.toLong
  }

  it should "throw from size with the 401 guidance message on 401" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt", Some(bearer("bad"))).size
    thrown.getMessage should include("HTTP 401 Unauthorized")
  }

  it should "throw from size with the 403 guidance message on 403" in {
    val thrown = the[Exception] thrownBy fs("/forbidden/file.txt", Some(bearer("bad"))).size
    thrown.getMessage should include("HTTP 403 Forbidden")
    thrown.getMessage should include("'repo' scope")
  }

  it should "return -1 from size when the server does not send Content-Length" in {
    // Mirrors servers using chunked transfer encoding (e.g. GitHub raw URLs).
    fs("/chunked/file.txt").size shouldBe -1L
  }

  // --- getParent edge cases --------------------------------------------

  it should "return None from getParent at the root URI" in {
    fs("/").getParent shouldBe None
  }

  // --- localize ---------------------------------------------------------

  // Endpoint that returns arbitrary binary bytes (0x00..0xFF) on GET.
  private val binaryBody: Array[Byte] = (0 to 255).map(_.toByte).toArray

  it should "preserve binary bytes when localizing a cached file (no String round-trip)" in {
    val source = fs("/binary/blob.bin")
    val _ = source.readBytes // populate the cache so the hasBytes branch is exercised
    val dest = java.nio.file.Files.createTempFile("auth-http-bin", ".bin")
    try {
      java.nio.file.Files.deleteIfExists(dest) // localize will (re)create it
      source.localize(dest, overwrite = true)
      java.nio.file.Files.readAllBytes(dest) shouldBe binaryBody
    } finally {
      java.nio.file.Files.deleteIfExists(dest)
    }
  }

  it should "create missing parent directories when localizing" in {
    val source = fs("/binary/blob.bin")
    val tempRoot = java.nio.file.Files.createTempDirectory("auth-http-loc")
    val dest = tempRoot.resolve("nested/dirs/that/do/not/exist/blob.bin")
    try {
      source.localize(dest)
      java.nio.file.Files.exists(dest) shouldBe true
      java.nio.file.Files.readAllBytes(dest) shouldBe binaryBody
    } finally {
      FileUtils.deleteRecursive(tempRoot)
    }
  }
}
