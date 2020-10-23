package dx.util.protocols

import java.nio.file.Files

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider
import dx.util.{CodecUtils, FileUtils}

class S3Test extends AnyFlatSpec with Matchers {
  private val testUri: String =
    "s3://1000genomes/phase3/data/HG00096/sequence_read/SRR062641.filt.fastq.gz"

  it should "resolve an S3 URI" in {
    val proto =
      S3FileAccessProtocol.create("us-east-1", Some(AnonymousCredentialsProvider.create()))
    proto.resolve(testUri) match {
      case fs: S3FileSource =>
        fs.name shouldBe "SRR062641.filt.fastq.gz"
        fs.bucketName shouldBe "1000genomes"
        fs.folder shouldBe "phase3/data/HG00096/sequence_read"
        fs.size shouldBe 9381826
        val tempDir = Files.createTempDirectory("test")
        tempDir.toFile.deleteOnExit()
        val localPath = fs.localizeToDir(tempDir)
        val content = CodecUtils.gzipDecompress(FileUtils.readFileBytes(localPath))
        content.slice(0, 10) shouldBe "@SRR062641"
      case other =>
        throw new Exception(s"expected an S3 file source, not ${other}")
    }
  }
}
