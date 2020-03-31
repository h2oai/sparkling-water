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

package ai.h2o.sparkling.extensions.rest.api

import ai.h2o.sparkling.extensions.internals.{CollectCategoricalDomainsTask, UpdateCategoricalIndicesTask}
import ai.h2o.sparkling.extensions.rest.api.schema.{FinalizeFrameV3, InitializeFrameV3, UploadPlanV3}
import ai.h2o.sparkling.utils.Base64Encoding
import water.{DKV, Key}
import water.api.Handler
import water.fvec.{ChunkUtils, Frame, Vec}
import water.parser.Categorical
import water.util.Log

class ImportFrameHandler extends Handler {
  def initialize(version: Int, request: InitializeFrameV3): InitializeFrameV3 = {
    ChunkUtils.initFrame(request.key, request.columns)
    request
  }

  def finalize(version: Int, request: FinalizeFrameV3): FinalizeFrameV3 = {
    val rowsPerChunk = Base64Encoding.decodeToLongArray(request.rows_per_chunk)
    val columnTypes = Base64Encoding.decode(request.column_types)
    val frameKey = Key.make(request.key)

    // Collect global domains from local domains on h2o nodes.
    val collectDomainsTask = new CollectCategoricalDomainsTask(frameKey)
    collectDomainsTask.doAllNodes()
    val stringDomains = collectDomainsTask.getDomains()
    val domains = expandDomains(stringDomains, columnTypes)

    ChunkUtils.finalizeFrame(request.key, rowsPerChunk, columnTypes, domains)

    // Update categorical indexes for all chunks.
    val frame = DKV.getGet[Frame](frameKey)
    val categoricalColumnIndices = for ((vec, idx) <- frame.vecs().zipWithIndex if vec.isCategorical) yield idx
    val updateCategoricalIndicesTask = new UpdateCategoricalIndicesTask(frameKey, categoricalColumnIndices)
    updateCategoricalIndicesTask.doAll(frame)

    // Convert categorical columns with too many categories to T_STR
    categoricalColumnIndices.foreach { catColIdx =>
      val vector = frame.vec(catColIdx)
      if (vector.cardinality() > Categorical.MAX_CATEGORICAL_COUNT) {
        Log.warn(
          s"Categorical column with index '$catColIdx' exceeded maximum number of categories. " +
            "Converting it to a column of strings ...")
        frame.replace(catColIdx, vector.toStringVec())
      }
    }

    request
  }

  private def expandDomains(stringDomains: Array[Array[String]], columnTypes: Array[Byte]): Array[Array[String]] = {
    var strIdx = 0
    var idx = 0
    val result = Array.fill[Array[String]](columnTypes.length)(null)
    while (idx < columnTypes.length) {
      if (columnTypes(idx) == Vec.T_CAT) {
        result(idx) = stringDomains(strIdx)
        strIdx += 1
      }
      idx += 1
    }
    result
  }

  def getUploadPlan(version: Int, request: UploadPlanV3): UploadPlanV3 = {
    val key = new Vec.VectorGroup().addVec()
    val layout = (0 until request.number_of_chunks).map { chunkId =>
      val h2oNode = Vec.chunkKey(key, chunkId).home_node
      new UploadPlanV3.ChunkAssigmentV3(chunkId, h2oNode)
    }.toArray
    request.layout = layout
    request
  }
}
