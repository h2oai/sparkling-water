/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package ai.h2o.sparkling.utils

import java.io.{InputStream, OutputStream}
import java.util.zip.{DeflaterInputStream, DeflaterOutputStream, GZIPInputStream, GZIPOutputStream}

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
      case "DEFLATE" => new DeflaterInputStream(inputStream)
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
