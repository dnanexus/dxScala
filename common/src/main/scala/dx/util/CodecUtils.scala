package dx.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.MessageDigest
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import javax.xml.bind.DatatypeConverter

object CodecUtils {
  // From: https://gist.github.com/owainlewis/1e7d1e68a6818ee4d50e
  // By: owainlewis
  def gzipCompress(bytes: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(bytes.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(bytes)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  def gzipDecompress(bytes: Array[Byte]): String = {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(bytes))
    scala.io.Source.fromInputStream(inputStream).mkString
  }

  def gzipAndBase64Encode(s: String): String = {
    val bytes = s.getBytes
    val gzBytes = gzipCompress(bytes)
    Base64.getEncoder.encodeToString(gzBytes)
  }

  def base64DecodeAndGunzip(s: String): String = {
    val ba: Array[Byte] = Base64.getDecoder.decode(s.getBytes)
    gzipDecompress(ba)
  }

  // Calculate the MD5 checksum of a string
  def md5Checksum(s: String): String = {
    val digest = MessageDigest.getInstance("MD5").digest(s.getBytes)
    digest.map("%02X" format _).mkString
  }

  def sha1Digest(s: String): String = {
    val md = MessageDigest.getInstance("SHA-1")
    DatatypeConverter.printHexBinary(md.digest(s.getBytes))
  }
}
