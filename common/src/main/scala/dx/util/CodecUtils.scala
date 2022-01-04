package dx.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.MessageDigest
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.io.Source

object CodecUtils {

  /**
    * Compresses an array of bytes using gzip.
    * Source: https://gist.github.com/owainlewis/1e7d1e68a6818ee4d50e
    */
  def gzipCompress(bytes: Array[Byte]): Array[Byte] = {
    val buf = new ByteArrayOutputStream(bytes.length)
    val gzip = new GZIPOutputStream(buf)
    gzip.write(bytes)
    gzip.close()
    buf.toByteArray
  }

  /**
    * Gzip-compresses and Base64-encodes a string.
    */
  def gzipAndBase64Encode(s: String): String = {
    Base64.getEncoder.encodeToString(gzipCompress(s.getBytes))
  }

  /**
    * Decompresses an array of gzip-compressed bytes to a string.
    */
  def gzipDecompress(bytes: Array[Byte]): String = {
    Source.fromInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes))).mkString
  }

  /**
    * Base64-decodes and decompresses a gzip-compressed and encoded string.
    */
  def base64DecodeAndGunzip(s: String): String = {
    gzipDecompress(Base64.getDecoder.decode(s.getBytes))
  }

  /**
    * Calculates the MD5 checksum of a string.
    */
  def md5Checksum(s: String): String = {
    MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02X".format(_)).mkString
  }

  /**
    * Calculates the SHA1 checksum of a string.
    */
  def sha1Digest(s: String): String = {
    MessageDigest.getInstance("SHA-1").digest(s.getBytes).map("%02X".format(_)).mkString
  }
}
