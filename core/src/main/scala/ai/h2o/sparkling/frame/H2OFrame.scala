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

import java.text.MessageFormat

import ai.h2o.sparkling.backend.utils.RestApiUtils._
import ai.h2o.sparkling.backend.utils.{RestCommunication, RestEncodingUtils}
import ai.h2o.sparkling.extensions.rest.api.Paths
import ai.h2o.sparkling.extensions.rest.api.schema.{FinalizeFrameV3, InitializeFrameV3}
import ai.h2o.sparkling.job.H2OJob
import ai.h2o.sparkling.utils.Base64Encoding
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext}
import water.api.schemas3.FrameChunksV3.FrameChunkV3
import water.api.schemas3.FrameV3.ColV3
import water.api.schemas3._

/**
 * H2OFrame representation via Rest API
 */
class H2OFrame private(val frameId: String, val columns: Array[H2OColumn], val chunks: Array[H2OChunk])
  extends Serializable with RestEncodingUtils {
  private val conf = H2OContext.ensure("H2OContext needs to be running in order to create H2OFrame").getConf
  val columnNames: Array[String] = columns.map(_.name)
  lazy val numberOfRows: Long = chunks.foldLeft(0L)((acc, chunk) => acc + chunk.numberOfRows)

  def numberOfColumns: Int = columns.length

  def convertAllStringColumnsToCategorical(): H2OFrame = {
    val columns = this.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    convertColumnsToCategorical(columns)
  }

  def convertColumnsToCategorical(columns: Array[String]): H2OFrame = {
    val endpoint = getClusterEndpoint(conf)
    val indices = columnNames.zipWithIndex.toMap
    val selectedIndices = columns.map { name =>
      indices.getOrElse(name, throw new IllegalArgumentException(s"Column $name does not exist in the frame $frameId"))
    }
    if (selectedIndices.isEmpty) {
      this
    } else {
      val params = Map(
        "ast" -> MessageFormat.format(s"( assign {0} (:= {0} (as.factor (cols {0} {1})) {1} []))", frameId, stringifyArray(selectedIndices))
      )
      val rapidsFrameV3 = update[RapidsFrameV3](endpoint, "99/Rapids", conf, params)
      H2OFrame(rapidsFrameV3.key.name)
    }
  }

  def splitToTrainAndValidationFrames(splitRatio: Double): Array[H2OFrame] = {
    if (splitRatio >= 1.0) {
      throw new IllegalArgumentException("Split ratio must be lower than 1.0")
    }
    val endpoint = getClusterEndpoint(conf)
    val params = Map(
      "ratios" -> Array(splitRatio),
      "dataset" -> frameId,
      "destination_frames" -> Array(s"${frameId}_train", s"${frameId}_valid")
    )
    val splitFrameV3 = update[SplitFrameV3](endpoint, "3/SplitFrame", conf, params)
    H2OJob(splitFrameV3.key.name).waitForFinish()
    splitFrameV3.destination_frames.map(frameKey => H2OFrame(frameKey.name))
  }

  def subframe(columns: Array[String]): H2OFrame = {
    val nonExistentColumns = columns.diff(columnNames)
    if (nonExistentColumns.nonEmpty) {
      throw new IllegalArgumentException(s"The following columns are not available on the H2OFrame ${this.frameId}: ${nonExistentColumns.mkString(", ")}")
    }
    if (columns.sorted.sameElements(columnNames.sorted)) {
      this
    } else {
      val endpoint = getClusterEndpoint(conf)
      val colIndices = columns.map(columnNames.indexOf)
      val newFrameId = s"${frameId}_subframe_${colIndices.mkString("_")}"
      val params = Map(
        "ast" -> MessageFormat.format(s"( assign {0} (cols {1} {2}))", newFrameId, frameId, stringifyArray(colIndices))
      )
      val rapidsFrameV3 = update[RapidsFrameV3](endpoint, "99/Rapids", conf, params)
      H2OFrame(rapidsFrameV3.key.name)
    }
  }
}

object H2OFrame extends RestCommunication {

  def apply(frameId: String): H2OFrame = {
    val conf = H2OContext.ensure().getConf
    getFrame(conf, frameId)
  }

  private def getFrame(conf: H2OConf, frameId: String): H2OFrame = {
    val endpoint = getClusterEndpoint(conf)
    val frames = query[FramesV3](
      endpoint,
      s"/3/Frames/$frameId/summary",
      conf,
      Map("row_count" -> 0),
      Seq((classOf[FrameV3], "chunk_summary"), (classOf[FrameV3], "distribution_summary")))
    val frame = frames.frames(0)
    val frameChunks = query[FrameChunksV3](endpoint, s"/3/FrameChunks/$frameId", conf)
    val clusterNodes = getNodes(getCloudInfoFromNode(endpoint, conf))

    new H2OFrame(
      frame.frame_id.name,
      frame.columns.map(convertColumn),
      frameChunks.chunks.map(convertChunk(_, clusterNodes)))
  }

  private def convertColumn(sourceColumn: ColV3): H2OColumn = {
    H2OColumn(
      name = sourceColumn.label,
      dataType = H2OColumnType.fromString(sourceColumn.`type`),
      min = sourceColumn.mins(0),
      max = sourceColumn.maxs(0),
      mean = sourceColumn.mean,
      sigma = sourceColumn.sigma,
      numberOfZeros = sourceColumn.zero_count,
      numberOfMissingElements = sourceColumn.missing_count,
      percentiles = sourceColumn.percentiles,
      domain = sourceColumn.domain,
      domainCardinality = sourceColumn.domain_cardinality)
  }

  private def convertChunk(sourceChunk: FrameChunkV3, clusterNodes: Array[NodeDesc]): H2OChunk = {
    H2OChunk(
      index = sourceChunk.chunk_id,
      numberOfRows = sourceChunk.row_count,
      location = clusterNodes(sourceChunk.node_idx))
  }

  private[sparkling] def initializeFrame(conf: H2OConf, frameId: String, columns: Array[String]): InitializeFrameV3 = {
    val endpoint = getClusterEndpoint(conf)
    val parameters = Map(
      "key" -> frameId,
      "columns" -> columns)
    update[InitializeFrameV3](endpoint, Paths.INITIALIZE_FRAME, conf, parameters)
  }

  private[sparkling] def finalizeFrame(
                                        conf: H2OConf,
                                        frameId: String,
                                        rowsPerChunk: Array[Long],
                                        columnTypes: Array[Byte]): FinalizeFrameV3 = {
    val endpoint = getClusterEndpoint(conf)
    val parameters = Map(
      "key" -> frameId,
      "rows_per_chunk" -> Base64Encoding.encode(rowsPerChunk),
      "column_types" -> Base64Encoding.encode(columnTypes))
    update[FinalizeFrameV3](endpoint, Paths.FINALIZE_FRAME, conf, parameters)
  }
}
