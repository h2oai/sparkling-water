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

package ai.h2o.sparkling.backend

import java.io.{InputStream, OutputStream}

import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.extensions.rest.api.Paths
import ai.h2o.sparkling.utils.{Base64Encoding, Compression}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.spark.h2o.H2OConf
import water.AutoBuffer


private[sparkling] case class H2OChunk(index: Int, numberOfRows: Int, location: NodeDesc)

private[sparkling] object H2OChunk extends RestCommunication {
  def getChunkAsInputStream(node: NodeDesc,
                            conf: H2OConf,
                            frameName: String,
                            numRows: Int,
                            chunkId: Int,
                            expectedTypes: Array[Byte],
                            selectedColumnsIndices: Array[Int]): InputStream = {
    val expectedTypesString = Base64Encoding.encode(expectedTypes)
    val selectedColumnsIndicesString = Base64Encoding.encode(selectedColumnsIndices)

    val parameters = Map(
      "frame_name" -> frameName,
      "num_rows" -> numRows,
      "chunk_id" -> chunkId,
      "expected_types" -> expectedTypesString,
      "selected_columns" -> selectedColumnsIndicesString,
      "compression" -> conf.externalCommunicationCompression)

    val endpoint = RestApiUtils.resolveNodeEndpoint(node, conf)
    val inputStream = readURLContent(endpoint, "POST", Paths.CHUNK, conf, parameters)
    Compression.decompress(conf.externalCommunicationCompression, inputStream)
  }

  def putChunk(node: NodeDesc,
               conf: H2OConf,
               frameName: String,
               numRows: Int,
               chunkId: Int,
               expectedTypes: Array[Byte],
               maxVecSizes: Array[Int]): OutputStream = {
    val expectedTypesString = Base64Encoding.encode(expectedTypes)
    val maxVecSizesString = Base64Encoding.encode(maxVecSizes)

    val parameters = Map(
      "frame_name" -> frameName,
      "num_rows" -> numRows,
      "chunk_id" -> chunkId,
      "expected_types" -> expectedTypesString,
      "maximum_vector_sizes" -> maxVecSizesString,
      "compression" -> conf.externalCommunicationCompression)

    val endpoint = RestApiUtils.resolveNodeEndpoint(node, conf)
    val addCompression =
      (outputStream: OutputStream) => Compression.compress(conf.externalCommunicationCompression, outputStream)
    insert(endpoint, Paths.CHUNK, conf, addCompression, parameters)
  }

  def putChunkCategoricalDomains(node: NodeDesc,
                                 conf: H2OConf,
                                 frameName: String,
                                 chunkId: Int,
                                 domains: Array[Array[String]]): Unit = {
    val parameters = Map(
      "frame_name" -> frameName,
      "chunk_id" -> chunkId,
      "compression" -> conf.externalCommunicationCompression)
    val endpoint = RestApiUtils.resolveNodeEndpoint(node, conf)
    val addCompression =
      (outputStream: OutputStream) => Compression.compress(conf.externalCommunicationCompression, outputStream)
    withResource(insert(endpoint, Paths.CHUNK_CATEGORICAL_DOMAINS, conf, addCompression, parameters)) { outputStream =>
      val autoBuffer = new AutoBuffer(outputStream, false)
      autoBuffer.putAAStr(domains)
      autoBuffer.close()
    }
  }
}
