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

  private var server: HttpServer = _
  private var baseUri: URI = _

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/ok", handler200)
    server.createContext("/unauthorized", handler401)
    server.createContext("/forbidden", handler403)
    server.createContext("/protected", handlerBearerProtected)
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
                 tokenEnvVarHint: Option[String] = None): AuthenticatedHttpFileSource = {
    val uri = baseUri.resolve(path)
    AuthenticatedHttpFileSource(uri,
                                StandardCharsets.UTF_8,
                                isDirectory = false,
                                credentials,
                                tokenEnvVarHint)(uri.toString)
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
    AuthenticatedHttpFileSource(uri,
                   StandardCharsets.UTF_8,
                   isDirectory = false,
                   None)(uri.toString).exists shouldBe false
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

  // --- tokenEnvVarHint propagation in the 401 message -------------------

  it should "include the supplied tokenEnvVarHint in the 401 guidance message" in {
    val thrown = the[Exception] thrownBy fs(
        "/unauthorized/file.txt",
        Some(bearer("bad")),
        tokenEnvVarHint = Some("MY_BEARER_TOKENS_VAR")
    ).exists
    thrown.getMessage should include("HTTP 401 Unauthorized")
    thrown.getMessage should include("MY_BEARER_TOKENS_VAR")
  }

  it should "fall back to a generic guidance message in the 401 message when no hint is supplied" in {
    val thrown = the[Exception] thrownBy fs(
        "/unauthorized/file.txt",
        Some(bearer("bad"))
    ).exists
    thrown.getMessage should include("HTTP 401 Unauthorized")
    thrown.getMessage should include("Supply Bearer credentials")
  }

  it should "propagate tokenEnvVarHint through resolve" in {
    val parent = fs("/ok/", Some(bearer("tok")), tokenEnvVarHint = Some("X"))
    parent.resolve("child.txt").tokenEnvVarHint shouldBe Some("X")
  }

  it should "propagate tokenEnvVarHint through resolveDirectory" in {
    val parent = fs("/ok/", Some(bearer("tok")), tokenEnvVarHint = Some("X"))
    parent.resolveDirectory("sub").tokenEnvVarHint shouldBe Some("X")
  }

  it should "propagate tokenEnvVarHint through getParent" in {
    val child = fs("/ok/dir/file.txt", Some(bearer("tok")), tokenEnvVarHint = Some("X"))
    child.getParent.flatMap(_.tokenEnvVarHint) shouldBe Some("X")
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
}
