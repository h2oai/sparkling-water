package ai.h2o.sparkling.utils

import java.io.{InputStream, OutputStream}
import java.util.zip.{DeflaterOutputStream, GZIPInputStream, GZIPOutputStream, InflaterInputStream}

import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

object Compression {
  val supportedCompressionTypes: Seq[String] = Seq("NONE", "DEFLATE", "GZIP", "SNAPPY")

  val defaultCompression: String = "SNAPPY"

  def compress(compressionType: String, outputStream: OutputStream): OutputStream = {
    validateCompressionType(compressionType) match {
      case "NONE" => outputStream
      case "DEFLATE" => new DeflaterOutputStream(outputStream)
      case "GZIP" => new GZIPOutputStream(outputStream)
      case "SNAPPY" => new SnappyOutputStream(outputStream)
    }
  }

  def decompress(compressionType: String, inputStream: InputStream): InputStream = {
    validateCompressionType(compressionType) match {
      case "NONE" => inputStream
      case "DEFLATE" => new InflaterInputStream(inputStream)
      case "GZIP" => new GZIPInputStream(inputStream)
      case "SNAPPY" => new SnappyInputStream(inputStream)
    }
  }

  def validateCompressionType(compressionType: String): String = {
    val upperCompressionType = compressionType.toUpperCase
    if (!supportedCompressionTypes.contains(upperCompressionType)) {
      throw new IllegalArgumentException("s'$compressionType' is not supported compression type.")
    }
    upperCompressionType
  }
}
