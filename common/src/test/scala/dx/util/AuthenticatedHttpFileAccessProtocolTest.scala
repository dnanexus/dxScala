package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI

class AuthenticatedHttpFileAccessProtocolTest extends AnyFlatSpec with Matchers {

  // --- parseTokens ------------------------------------------------------

  it should "parse an empty string into an empty map" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("") shouldBe empty
  }

  it should "parse a single domain:token entry" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("foo.com:abc") shouldBe Map("foo.com" -> "abc")
  }

  it should "parse multiple entries separated by semicolons" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("foo.com:abc;bar.com:xyz") shouldBe
      Map("foo.com" -> "abc", "bar.com" -> "xyz")
  }

  it should "lowercase the domain but preserve token case" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("FOO.com:AbCdEf") shouldBe Map("foo.com" -> "AbCdEf")
  }

  it should "trim surrounding whitespace from entries, domains, and tokens" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("  foo.com : abc  ;  bar.com : xyz  ") shouldBe
      Map("foo.com" -> "abc", "bar.com" -> "xyz")
  }

  it should "split on the first colon only so tokens may contain colons" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("foo.com:abc:def:ghi") shouldBe
      Map("foo.com" -> "abc:def:ghi")
  }

  it should "skip entries with no colon" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens("foo.com;bar.com:xyz") shouldBe Map("bar.com" -> "xyz")
  }

  it should "skip entries with empty domain or empty token" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens(":token;domain:;real.com:tok") shouldBe
      Map("real.com" -> "tok")
  }

  it should "ignore empty segments from leading/trailing/duplicate semicolons" in {
    AuthenticatedHttpFileAccessProtocol.parseTokens(";;foo.com:abc;;;bar.com:xyz;;") shouldBe
      Map("foo.com" -> "abc", "bar.com" -> "xyz")
  }

  it should "keep the last value when the same domain appears twice" in {
    // `.toMap` on a duplicate-key sequence keeps the last
    AuthenticatedHttpFileAccessProtocol.parseTokens("foo.com:first;foo.com:second") shouldBe
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
    emptyProtocol.resolveDirectory("https://raw.githubusercontent.com/dir/").credentials shouldBe None
  }

  it should "preserve the original address as the FileSource address" in {
    val address = "https://raw.githubusercontent.com/owner/repo/main/x.wdl"
    protocolWithTokens.resolve(address).address shouldBe address
  }

  it should "use the URI form as address when resolved via the URI overload without an explicit value" in {
    val uri = URI.create("https://raw.githubusercontent.com/owner/repo/main/x.wdl")
    protocolWithTokens.resolve(uri).address shouldBe uri.toString
  }

  // --- fromEnvironment --------------------------------------------------

  it should "expose the correct env variable name" in {
    AuthenticatedHttpFileAccessProtocol.TokensEnvVar shouldBe "WDL_IMPORT_BEARER_TOKENS"
  }

  it should "return a protocol with no tokens when WDL_IMPORT_BEARER_TOKENS is not set" in {
    // We can't portably mutate process env vars on JVM 11, so we only verify
    // the unset path. The set path is logically equivalent to parseTokens(value)
    // followed by direct construction, both already covered above.
    assume(sys.env.get(AuthenticatedHttpFileAccessProtocol.TokensEnvVar).isEmpty,
           "WDL_IMPORT_BEARER_TOKENS is set in the test environment; skipping unset-path test")
    val protocol = AuthenticatedHttpFileAccessProtocol.fromEnvironment()
    protocol.domainBearerTokens shouldBe empty
    protocol.resolve("https://raw.githubusercontent.com/x.wdl").credentials shouldBe None
  }

  it should "use the default encoding and a Quiet logger when fromEnvironment is called with no args" in {
    assume(sys.env.get(AuthenticatedHttpFileAccessProtocol.TokensEnvVar).isEmpty)
    val protocol = AuthenticatedHttpFileAccessProtocol.fromEnvironment()
    protocol.encoding shouldBe FileUtils.DefaultEncoding
    protocol.logger shouldBe Logger.Quiet
  }
}
