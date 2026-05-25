package dx.util

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

class HttpFileSourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

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
                 auth: Option[HttpFileAuthentication] = None): HttpFileSource = {
    val uri = baseUri.resolve(path)
    HttpFileSource(uri,
                   StandardCharsets.UTF_8,
                   isDirectory = false,
                   auth)(uri.toString)
  }

  private def bearerAuth(token: String): HttpFileAuthentication =
    HttpFileAuthentication(AuthType.Bearer, token)

  private def lastAuthHeader(): Option[String] = {
    authHeadersSeen.asScala.lastOption.flatten
  }

  // --- AuthType ---------------------------------------------------------

  it should "expose a Bearer AuthType with non-empty messages" in {
    AuthType.Bearer.unauthorizedMessage should not be empty
    AuthType.Bearer.forbiddenMessage should not be empty
  }

  // --- Without auth -----------------------------------------------------

  it should "return true from exists when the server responds 200" in {
    authHeadersSeen.clear()
    fs("/ok/file.txt").exists shouldBe true
    lastAuthHeader() shouldBe None
  }

  it should "return false from exists on 401 when no auth is configured" in {
    fs("/unauthorized/file.txt").exists shouldBe false
  }

  it should "return false from exists on 403 when no auth is configured" in {
    fs("/forbidden/file.txt").exists shouldBe false
  }

  it should "return false from exists when the host cannot be resolved" in {
    val uri = URI.create("http://no-such-host.invalid./missing.txt")
    HttpFileSource(uri, StandardCharsets.UTF_8, isDirectory = false, None)(uri.toString).exists shouldBe false
  }

  it should "read bytes from a 200 response when no auth is configured" in {
    new String(fs("/ok/file.txt").readBytes, StandardCharsets.UTF_8) shouldBe "hello"
  }

  it should "throw from readBytes on a 401 response when no auth is configured" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt").readBytes
    thrown.getMessage should include("Error fetching URL")
    thrown.getMessage should include("401")
  }

  // --- With Bearer auth -------------------------------------------------

  it should "send an Authorization: Bearer header when bearer auth is configured" in {
    authHeadersSeen.clear()
    fs("/ok/file.txt", Some(bearerAuth("abc"))).exists shouldBe true
    lastAuthHeader() shouldBe Some("Bearer abc")
  }

  it should "succeed against a bearer-protected endpoint when the token matches" in {
    fs("/protected/file.txt", Some(bearerAuth(expectedToken))).exists shouldBe true
  }

  it should "throw from exists with the Bearer 401 message on 401 when auth is configured" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt", Some(bearerAuth("bad"))).exists
    thrown.getMessage should include("401")
    thrown.getMessage should include(AuthType.Bearer.unauthorizedMessage)
  }

  it should "throw from exists with the Bearer 403 message on 403 when auth is configured" in {
    val thrown = the[Exception] thrownBy fs("/forbidden/file.txt", Some(bearerAuth("bad"))).exists
    thrown.getMessage should include("403")
    thrown.getMessage should include(AuthType.Bearer.forbiddenMessage)
  }

  it should "throw from readBytes with the Bearer 401 message on 401 when auth is configured" in {
    val thrown = the[Exception] thrownBy fs("/unauthorized/file.txt", Some(bearerAuth("bad"))).readBytes
    thrown.getMessage should include(AuthType.Bearer.unauthorizedMessage)
  }

  // --- auth propagation -------------------------------------------------

  it should "propagate auth through resolve" in {
    val parent = fs("/ok/", Some(bearerAuth("tok")))
    parent.resolve("child.txt").auth shouldBe Some(bearerAuth("tok"))
  }

  it should "propagate auth through resolveDirectory" in {
    val parent = fs("/ok/", Some(bearerAuth("tok")))
    parent.resolveDirectory("sub").auth shouldBe Some(bearerAuth("tok"))
  }

  it should "propagate auth through getParent" in {
    val child = fs("/ok/dir/file.txt", Some(bearerAuth("tok")))
    child.getParent.flatMap(_.auth) shouldBe Some(bearerAuth("tok"))
  }
}
