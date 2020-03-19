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

import ai.h2o.sparkling.extensions.rest.api.schema.{FinalizeFrameV3, InitializeFrameV3, UploadPlanV3}
import ai.h2o.sparkling.utils.Base64Encoding
import water.{DKV, H2O, Key, RPC}
import water.api.Handler
import water.fvec.{Vec, ChunkUtils, Frame}
import water.parser.{CollectCategoricalDomainsTask, CreateParse2GlobalCategoricalMaps}
import water.parser.ParseDataset.MultiFileParseTask

class ImportFrameHandler extends Handler {
  def initialize(version: Int, request: InitializeFrameV3): InitializeFrameV3 = {
    ChunkUtils.initFrame(request.key, request.columns)
    request
  }

  def finalize(version: Int, request: FinalizeFrameV3): FinalizeFrameV3 = {
    val rowsPerChunk = Base64Encoding.decodeToLongArray(request.rows_per_chunk)
    val columnTypes = Base64Encoding.decode(request.column_types)

    val collectDomainsTask = new CollectCategoricalDomainsTask(Key.make(request.key))
    collectDomainsTask.doAllNodes()
    val domains = collectDomainsTask.getDomains()

    ChunkUtils.finalizeFrame(request.key, rowsPerChunk, columnTypes, domains)
    request
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

  private def updateCategoricalDomainReferences(frameName: String)= {
    val frame = DKV.getGet[Frame](frameName)
    val numberOfChunks = frame.anyVec().nChunks()
    frame.anyVec()._cids
    val fcdt = new Array[CreateParse2GlobalCategoricalMaps](H2O.CLOUD.size)
    val rpcs = new Array[RPC[_ <: DTask[_]]](H2O.CLOUD.size)
    var i = 0
    while ( {
      i < fcdt.length
    }) {
      val nodes = H2O.CLOUD.members
      fcdt(i) = new ParseDataset.CreateParse2GlobalCategoricalMaps(mfpt._cKey, fr._key, ecols, mfpt._parseSetup._parse_columns_indices)
      rpcs(i) = new RPC[ParseDataset.CreateParse2GlobalCategoricalMaps](nodes(i), fcdt(i)).call

      {
        i += 1;
        i - 1
      }
    }
    for (rpc <- rpcs) {
      rpc.get
    }

    new ParseDataset.UpdateCategoricalChunksTask(mfpt._cKey, mfpt._chunk2ParseNodeMap).doAll(evecs)
    MultiFileParseTask._categoricals.remove(mfpt._cKey)
  }
}
