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

package water.fvec

import org.apache.spark.h2o.utils.NodeDesc


/**
 * This is a simple bridge to access package-private/protected members.
 */
trait FrameUtils {

  /** @see Frame#preparePartialFrame */
  def preparePartialFrame(fr: Frame, names: Array[String]): Unit = {
    fr.preparePartialFrame(names)
  }

  /** @see Frame#finalizePartialFrame */
  def finalizePartialFrame(fr: Frame, rowsPerChunk: Array[Long],
                           colDomains: Array[Array[String]],
                           colTypes: Array[Byte]): Unit = {
    fr.finalizePartialFrame(rowsPerChunk, colDomains, colTypes)
  }

  // TODO(vlad): Introduce implicit value class that ensures chunks compatibility
  /** @see Frame#createNewChunks */
  def createNewChunks(name: String, vecTypes: Array[Byte], cidx: Int): Array[NewChunk] =
    Frame.createNewChunks(name, vecTypes, cidx)

  /** @see Frame#closeNewChunks */
  def closeNewChunks(nchks: Array[NewChunk]): Unit = Frame.closeNewChunks(nchks)

  def getChunks(fr: Frame, cidx: Int): Array[Chunk] = {
    val vecs = fr.vecs()
    val chks = vecs.indices.map(idx => vecs(idx).chunkForChunkIdx(cidx))
    chks.toArray
  }

  /**
    * Get the home nodes of chunks. Since all the Vecs in a Frame belong to the same Vec.VectorGroup we can only ask one vec
    * for its chunks' home nodes (it is then same for the rest of the vectors)
    * @param fr frame on which determine home nodes of chunks
    * @return mapping of chunk index to node description
    */
  def getChunksLocations(fr: Frame): Array[NodeDesc] = {
    val chunkCount = fr.anyVec().nChunks()
    val cidxToH2ONode = new Array[NodeDesc](chunkCount)
    (0 until chunkCount).foreach { cidx =>
      cidxToH2ONode(cidx) = NodeDesc(fr.anyVec().chunkKey(cidx).home_node())
    }
    cidxToH2ONode
  }
}

object FrameUtils extends FrameUtils
