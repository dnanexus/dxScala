package dx.util

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{InetSocketAddress, URI}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class HttpFileSourceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val binaryBody: Array[Byte] = (0 to 255).map(_.toByte).toArray

  private def respond(exchange: HttpExchange, status: Int, body: Array[Byte]): Unit = {
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

  private var server: HttpServer = _
  private var baseUri: URI = _

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/binary", (exchange: HttpExchange) => respond(exchange, 200, binaryBody))
    // Refuses HEAD with 405 but serves GET normally — mirrors servers that
    // selectively register methods (RFC 7231 §6.5.5).
    server.createContext(
        "/head405",
        (exchange: HttpExchange) =>
          if (exchange.getRequestMethod == "HEAD") {
            exchange.getResponseHeaders.set("Allow", "GET")
            exchange.sendResponseHeaders(405, -1)
            exchange.close()
          } else respond(exchange, 200, binaryBody)
    )
    server.setExecutor(null)
    server.start()
    baseUri = URI.create(s"http://127.0.0.1:${server.getAddress.getPort}")
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
  }

  private def fs(path: String): HttpFileSource = {
    val uri = baseUri.resolve(path)
    HttpFileSource(uri, StandardCharsets.UTF_8, isDirectory = false)(uri.toString)
  }

  it should "return None from getParent at the root URI" in {
    fs("/").getParent shouldBe None
  }

  it should "preserve binary bytes when localizing a cached file (no String round-trip)" in {
    val source = fs("/binary/blob.bin")
    val _ = source.readBytes // populate the cache so the hasBytes branch is exercised
    val dest = Files.createTempFile("http-bin", ".bin")
    try {
      Files.deleteIfExists(dest)
      source.localize(dest, overwrite = true)
      Files.readAllBytes(dest) shouldBe binaryBody
    } finally {
      Files.deleteIfExists(dest)
    }
  }

  it should "create missing parent directories when localizing" in {
    val source = fs("/binary/blob.bin")
    val tempRoot = Files.createTempDirectory("http-loc")
    val dest = tempRoot.resolve("nested/dirs/that/do/not/exist/blob.bin")
    try {
      source.localize(dest)
      Files.exists(dest) shouldBe true
      Files.readAllBytes(dest) shouldBe binaryBody
    } finally {
      FileUtils.deleteRecursive(tempRoot)
    }
  }

  it should "transparently retry with GET when the server rejects HEAD with 405" in {
    fs("/head405/file.bin").exists shouldBe true
    fs("/head405/file.bin").size shouldBe binaryBody.length.toLong
  }
}
