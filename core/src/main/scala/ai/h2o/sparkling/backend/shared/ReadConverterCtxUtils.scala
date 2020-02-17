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

package ai.h2o.sparkling.backend.shared

import ai.h2o.sparkling.backend.external.ExternalReadConverterCtx
import ai.h2o.sparkling.backend.internal.InternalReadConverterCtx
import ai.h2o.sparkling.frame.H2OChunk
import org.apache.spark.h2o.H2OConf

private[backend] object ReadConverterCtxUtils {

  def create(keyName: String, chunkIdx: Int,
             chunkLocations: Option[Array[H2OChunk]],
             expectedTypes: Option[Array[Byte]],
             selectedColumnIndices: Array[Int],
             conf: H2OConf): ReadConverterCtx = {

    chunkLocations.map { chunks =>
      val chnk = chunks.find(_.index == chunkIdx).head
      new ExternalReadConverterCtx(keyName, chunkIdx, chnk.numberOfRows, chnk.location, expectedTypes.get, selectedColumnIndices, conf)
    }
      .getOrElse(new InternalReadConverterCtx(keyName, chunkIdx))

  }

  /**
   * In case of internal backend the method returns original iterator.
   *
   * In case of external backend the iterator is buffered ( = all elements are downloaded from remote h2o node )
   * so they are available locally and are provided as a new iterator
   */
  def backendSpecificIterator[T](runningOnExternalBackend: Boolean,
                                 iterator: Iterator[T]): Iterator[T] = {
    if (runningOnExternalBackend) {
      // When user ask to read whatever number of rows, buffer them all, because we can't keep the connection
      // to h2o opened indefinitely
      iterator.toList.toIterator
    } else {
      iterator
    }
  }

}
