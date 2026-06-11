package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI

class AuthenticatedHttpFileAccessProtocolTest extends AnyFlatSpec with Matchers {

  // --- parseAuthTokens ------------------------------------------------------

  it should "parse an empty string into an empty map" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("") shouldBe empty
  }

  it should "parse a single domain:token entry" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("foo.com:abc") shouldBe Map("foo.com" -> "abc")
  }

  it should "parse multiple entries separated by semicolons" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("foo.com:abc;bar.com:xyz") shouldBe
      Map("foo.com" -> "abc", "bar.com" -> "xyz")
  }

  it should "lowercase the domain but preserve token case" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("FOO.com:AbCdEf") shouldBe Map(
        "foo.com" -> "AbCdEf"
    )
  }

  it should "trim surrounding whitespace from entries, domains, and tokens" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("  foo.com : abc  ;  bar.com : xyz  ") shouldBe
      Map("foo.com" -> "abc", "bar.com" -> "xyz")
  }

  it should "split on the first colon only so tokens may contain colons" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("foo.com:abc:def:ghi") shouldBe
      Map("foo.com" -> "abc:def:ghi")
  }

  it should "skip entries with no colon" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("foo.com;bar.com:xyz") shouldBe Map(
        "bar.com" -> "xyz"
    )
  }

  it should "skip entries with empty domain or empty token" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens(":token;domain:;real.com:tok") shouldBe
      Map("real.com" -> "tok")
  }

  it should "ignore empty segments from leading/trailing/duplicate semicolons" in {
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens(";;foo.com:abc;;;bar.com:xyz;;") shouldBe
      Map("foo.com" -> "abc", "bar.com" -> "xyz")
  }

  it should "keep the last value when the same domain appears twice" in {
    // `.toMap` on a duplicate-key sequence keeps the last
    AuthenticatedHttpFileAccessProtocol.parseAuthTokens("foo.com:first;foo.com:second") shouldBe
      Map("foo.com" -> "second")
  }

  // --- AuthenticatedHttpFileAccessProtocol auth attachment ---------------------------

  private val protocolWithTokens = AuthenticatedHttpFileAccessProtocol(
      domainBearerTokens = Map("raw.githubusercontent.com" -> "tok-1", "other.com" -> "tok-2")
  )

  it should "attach Bearer auth on resolve when the host matches a configured domain" in {
    val fs = protocolWithTokens.resolve("https://raw.githubusercontent.com/owner/repo/main/x.wdl")
    fs.credentials shouldBe Some(HttpCredentials(HttpAuthenticationScheme.Bearer, "tok-1"))
  }

  it should "match domains case-insensitively when attaching auth" in {
    val fs = protocolWithTokens.resolve("https://RAW.GitHubUserContent.com/x.wdl")
    fs.credentials shouldBe Some(HttpCredentials(HttpAuthenticationScheme.Bearer, "tok-1"))
  }

  it should "not attach auth when the host is not in the configured tokens" in {
    val fs = protocolWithTokens.resolve("https://unknown.example/x.wdl")
    fs.credentials shouldBe None
  }

  it should "use only the host for matching, ignoring path/query" in {
    val fs = protocolWithTokens.resolve("https://other.com/path?q=raw.githubusercontent.com")
    fs.credentials shouldBe Some(HttpCredentials(HttpAuthenticationScheme.Bearer, "tok-2"))
  }

  it should "attach auth on resolveDirectory the same way as resolve" in {
    val dir = protocolWithTokens.resolveDirectory("https://raw.githubusercontent.com/owner/repo/")
    dir.credentials shouldBe Some(HttpCredentials(HttpAuthenticationScheme.Bearer, "tok-1"))
    dir.isDirectory shouldBe true
  }

  it should "leave auth as None for any URI when no tokens are configured" in {
    val emptyProtocol = AuthenticatedHttpFileAccessProtocol()
    emptyProtocol.resolve("https://raw.githubusercontent.com/x.wdl").credentials shouldBe None
    emptyProtocol
      .resolveDirectory("https://raw.githubusercontent.com/dir/")
      .credentials shouldBe None
  }

  it should "preserve the original address as the FileSource address" in {
    val address = "https://raw.githubusercontent.com/owner/repo/main/x.wdl"
    protocolWithTokens.resolve(address).address shouldBe address
  }

  it should "use the URI form as address when resolved via the URI overload without an explicit value" in {
    val uri = URI.create("https://raw.githubusercontent.com/owner/repo/main/x.wdl")
    protocolWithTokens.resolve(uri).address shouldBe uri.toString
  }

  // --- unauthorizedHint propagation --------------------------------------

  it should "default unauthorizedHint to None and forward None to constructed sources" in {
    val protocol = AuthenticatedHttpFileAccessProtocol()
    protocol.unauthorizedHint shouldBe None
    protocol.resolve("https://example.com/x.wdl").unauthorizedHint shouldBe None
    protocol.resolveDirectory("https://example.com/dir/").unauthorizedHint shouldBe None
  }

  it should "forward unauthorizedHint to constructed sources via resolve" in {
    val protocol = AuthenticatedHttpFileAccessProtocol(
        unauthorizedHint = Some("MY_TOKENS_VAR")
    )
    protocol.resolve("https://example.com/x.wdl").unauthorizedHint shouldBe Some("MY_TOKENS_VAR")
  }

  it should "forward unauthorizedHint to constructed sources via resolveDirectory" in {
    val protocol = AuthenticatedHttpFileAccessProtocol(
        unauthorizedHint = Some("MY_TOKENS_VAR")
    )
    protocol
      .resolveDirectory("https://example.com/dir/")
      .unauthorizedHint shouldBe Some("MY_TOKENS_VAR")
  }
}
