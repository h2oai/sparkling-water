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

package ai.h2o.sparkling.frame

import java.io.InputStream

import ai.h2o.sparkling.extensions.rest.api.Paths
import ai.h2o.sparkling.utils.{Base64Encoding, RestApiUtils, RestCommunication}
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.NodeDesc


case class H2OChunk(index: Int, numberOfRows: Long, location: NodeDesc)

object H2OChunk extends RestCommunication {
  def getChunkAsInputStream(
                             node: NodeDesc,
                             conf: H2OConf,
                             frameName: String,
                             chunkId: Int,
                             expectedTypes: Array[Byte],
                             selectedColumnsIndices: Array[Int]): InputStream = {
    val expectedTypesString = Base64Encoding.encode(expectedTypes)
    val selectedColumnsIndicesString = Base64Encoding.encode(selectedColumnsIndices)

    val parameters = Map(
      "frame_name" -> frameName,
      "chunk_id" -> chunkId.toString,
      "expected_types" -> expectedTypesString,
      "selected_columns" -> selectedColumnsIndicesString)

    val endpoint = RestApiUtils.resolveNodeEndpoint(node, conf)
    readURLContent(endpoint, "GET", Paths.CHUNK, conf, parameters)
  }
}
