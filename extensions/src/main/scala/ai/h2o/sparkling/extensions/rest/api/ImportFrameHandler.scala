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

import ai.h2o.sparkling.extensions.internals.{CollectCategoricalDomainsTask, ConvertCategoricalToStringColumnsTask, UpdateCategoricalIndicesTask}
import ai.h2o.sparkling.extensions.rest.api.schema.{FinalizeFrameV3, InitializeFrameV3, UploadPlanV3}
import ai.h2o.sparkling.utils.Base64Encoding
import water.{DKV, Key}
import water.api.Handler
import water.fvec.{AppendableVec, ChunkUtils, Frame, Vec}
import water.parser.CategoricalPreviewParseWriter
import water.util.Log

import scala.collection.mutable.ArrayBuffer

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
    val isEmpty = stringDomains == null
    val columnTypesAwareOfEmptyFrames = if (isEmpty) {
      columnTypes.map(columnType => if (columnType == Vec.T_CAT) Vec.T_STR else columnType)
    } else {
      columnTypes
    }
    val domains = expandDomains(stringDomains, columnTypesAwareOfEmptyFrames)
    ChunkUtils.finalizeFrame(request.key, rowsPerChunk, columnTypesAwareOfEmptyFrames, domains)
    val frame = DKV.getGet[Frame](frameKey)

    convertColumnsWithTooManyCategoricalLevelsToStringColumns(frame, columnTypesAwareOfEmptyFrames, stringDomains)

    val categoricalColumnIndices = for ((vec, idx) <- frame.vecs().zipWithIndex if vec.isCategorical) yield idx

    // Update categorical indices for all chunks. (local chunk domains -> global domains)
    val updateCategoricalIndicesTask = new UpdateCategoricalIndicesTask(frameKey, categoricalColumnIndices)
    updateCategoricalIndicesTask.doAll(frame)

    convertCategoricalColumnsToOtherTypesIfNeeded(frame, categoricalColumnIndices)

    DKV.put(frame)
    request
  }

  private def convertColumnsWithTooManyCategoricalLevelsToStringColumns(
      frame: Frame,
      columnTypes: Array[Byte],
      stringDomains: Array[Array[String]]) = {
    val indicesOfChanges = indicesOfChangedColumnsFromCategoricalToString(columnTypes, stringDomains)
    if (indicesOfChanges.nonEmpty) {
      indicesOfChanges.foreach { idx =>
        Log.info(
          s"A categorical column '${frame.names()(idx)}' exceeded maximum number of categories. " +
            "Converting it to a column of strings ...")
      }
      val vectorsToBeConvertedToString = indicesOfChanges.map(frame.vec(_))
      val domainIndices = for ((domain, idx) <- stringDomains.zipWithIndex if domain == null) yield idx
      val convertCategoricalToStringColumnsTask = new ConvertCategoricalToStringColumnsTask(frame._key, domainIndices)
      convertCategoricalToStringColumnsTask.doAll(Vec.T_STR, vectorsToBeConvertedToString: _*)
      val newVectors = AppendableVec.closeAll(convertCategoricalToStringColumnsTask.appendables())
      for ((newVector, index) <- newVectors.zip(indicesOfChanges)) {
        val oldVector = frame.replace(index, newVector)
        oldVector.remove()
      }
    }
  }

  private def convertCategoricalColumnsToOtherTypesIfNeeded(frame: Frame, categoricalColumnIndices: Array[Int]) = {
    categoricalColumnIndices.foreach { idx =>
      val vector = frame.vec(idx)
      val previewWriter =
        new CategoricalPreviewParseWriter(vector.domain(), vector.length().toInt, vector.naCnt().toInt)
      val types = previewWriter.guessTypes()
      types(0) match {
        case Vec.T_CAT => // No action needed
        case Vec.T_STR =>
          Log.info(s"The categorical column '${frame.names()(idx)}' has been converted to string.")
          val oldVector = frame.replace(idx, vector.toStringVec())
          oldVector.remove()
        case Vec.T_NUM =>
          Log.info(s"The categorical column '${frame.names()(idx)}' has been converted to numeric.")
          val oldVector = frame.replace(idx, vector.toNumericVec())
          oldVector.remove()
        case unexpectedType =>
          Log.err(
            s"The categorical column '${frame.names()(idx)}' has been identified to be " +
              s"type of '${unexpectedType}', but the conversion is not supported. ")
      }
    }
  }

  private def indicesOfChangedColumnsFromCategoricalToString(
      columnTypes: Array[Byte],
      stringDomains: Array[Array[String]]): Array[Int] = {
    var strIdx = 0
    var idx = 0
    val result = ArrayBuffer[Int]()
    while (idx < columnTypes.length) {
      if (columnTypes(idx) == Vec.T_CAT) {
        if (stringDomains(strIdx) == null) {
          result.append(idx)
        }
        strIdx += 1
      }
      idx += 1
    }
    result.toArray
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
