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

package ai.h2o.sparkling

import java.text.MessageFormat

import ai.h2o.sparkling.backend.utils.RestApiUtils._
import ai.h2o.sparkling.backend.utils.{RestCommunication, RestEncodingUtils}
import ai.h2o.sparkling.backend.{H2OChunk, H2OJob, NodeDesc}
import ai.h2o.sparkling.extensions.rest.api.Paths
import ai.h2o.sparkling.extensions.rest.api.schema.{FinalizeFrameV3, InitializeFrameV3}
import ai.h2o.sparkling.utils.Base64Encoding
import org.apache.spark.h2o.{H2OConf, H2OContext}
import water.api.schemas3.FrameChunksV3.FrameChunkV3
import water.api.schemas3.FrameV3.ColV3
import water.api.schemas3._

import scala.util.Random

/**
  * H2OFrame representation via Rest API
  */
class H2OFrame private (
    val frameId: String,
    val columns: Array[H2OColumn],
    private[sparkling] val chunks: Array[H2OChunk])
  extends Serializable
  with RestEncodingUtils {
  private val conf = H2OContext.ensure("H2OContext needs to be running in order to create H2OFrame").getConf
  val columnNames: Array[String] = columns.map(_.name)
  lazy val numberOfRows: Long = chunks.foldLeft(0L)((acc, chunk) => acc + chunk.numberOfRows)

  def numberOfColumns: Int = columns.length

  def convertAllStringColumnsToCategorical(): H2OFrame = {
    val columns = this.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    convertColumnsToCategorical(columns)
  }

  def convertColumnsToCategorical(columns: Array[String]): H2OFrame = {
    val indices = columnNames.zipWithIndex.toMap
    val selectedIndices = columns.map { name =>
      indices.getOrElse(name, throw new IllegalArgumentException(s"Column $name does not exist in the frame $frameId"))
    }
    convertColumnsToCategorical(selectedIndices)
  }

  def convertColumnsToCategorical(columnIndices: Array[Int]): H2OFrame = {
    val nonExisting = columnIndices.filterNot(columns.indices.contains(_))
    if (nonExisting.nonEmpty) {
      throw new IllegalArgumentException(
        s"Columns with indices ${nonExisting.mkString("[", ",", "]")} are not in the frame $frameId." +
          s" The frame has ${columnNames.length} columns.")
    }
    val endpoint = getClusterEndpoint(conf)
    if (columnIndices.isEmpty) {
      this
    } else {
      val params = Map(
        "ast" -> MessageFormat
          .format(s"( assign {0} (:= {0} (as.factor (cols {0} {1})) {1} []))", frameId, stringifyArray(columnIndices)))
      val rapidsFrameV3 = update[RapidsFrameV3](endpoint, "99/Rapids", conf, params)
      H2OFrame(rapidsFrameV3.key.name)
    }
  }

  def split(splitRatios: Double*): Array[H2OFrame] = {
    if (splitRatios.sum >= 1.0) {
      throw new IllegalArgumentException("Split ratios must be lower than 1.0")
    }
    val endpoint = getClusterEndpoint(conf)
    val params = Map("ratios" -> splitRatios.toArray, "dataset" -> frameId)
    val splitFrameV3 = update[SplitFrameV3](endpoint, "3/SplitFrame", conf, params)
    H2OJob(splitFrameV3.key.name).waitForFinish()
    splitFrameV3.destination_frames.map(frameKey => H2OFrame(frameKey.name))
  }

  def subframe(columns: Array[String]): H2OFrame = {
    val nonExistentColumns = columns.diff(columnNames)
    if (nonExistentColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"The following columns are not available on the H2OFrame ${this.frameId}: ${nonExistentColumns.mkString(", ")}")
    }
    if (columns.sorted.sameElements(columnNames.sorted)) {
      this
    } else {
      val endpoint = getClusterEndpoint(conf)
      val colIndices = columns.map(columnNames.indexOf)
      val newFrameId = s"${frameId}_subframe_${colIndices.mkString("_")}"
      val params = Map(
        "ast" -> MessageFormat.format(s"( assign {0} (cols {1} {2}))", newFrameId, frameId, stringifyArray(colIndices)))
      val rapidsFrameV3 = update[RapidsFrameV3](endpoint, "99/Rapids", conf, params)
      H2OFrame(rapidsFrameV3.key.name)
    }
  }

  /**
    * Left join this frame with another frame
    *
    * @param another right frame
    * @return new frame
    */
  def leftJoin(another: H2OFrame): H2OFrame =
    join(another, allFromCurrent = true, allFromAnother = false, "radix")

  /**
    * Right join this frame with another frame
    *
    * @param another right frame
    * @return new frame
    */
  def rightJoin(another: H2OFrame, method: String = "AUTO"): H2OFrame = {
    // Right join fails under "radix". The other variant is "hash" method
    // but that method does not support strings in columns and does not work correctly if there are duplicate
    // columns in the frame. Use Spark for now
    joinUsingSpark(another, "right")
  }

  /**
    * Inner join this frame with another frame
    *
    * @param another right frame
    * @return new frame
    */
  def innerJoin(another: H2OFrame): H2OFrame =
    join(another, allFromCurrent = false, allFromAnother = false, "radix")

  /**
    * Outer join this frame with another frame
    *
    * @param another right frame
    * @return new frame
    */
  def outerJoin(another: H2OFrame): H2OFrame = {
    // Outer join is broken in H2O, simulate H2O's join via Spark for now
    joinUsingSpark(another, "outer")
  }

  private def joinUsingSpark(another: H2OFrame, method: String): H2OFrame = {
    val hc = H2OContext.ensure()
    val currentFrame = hc.asDataFrame(this.frameId)
    val anotherFrame = hc.asDataFrame(another.frameId)
    val sameCols = anotherFrame.columns.intersect(currentFrame.columns)
    val joined = currentFrame.join(anotherFrame, sameCols, method)
    H2OFrame(hc.asH2OFrameKeyString(joined))
  }

  /**
    * Join this frame with another frame
    *
    * @param another        right frame
    * @param allFromCurrent all values from current frame
    * @param allFromAnother all values from another frame
    * @param method         joining method
    * @return
    */
  private def join(
      another: H2OFrame,
      allFromCurrent: Boolean = false,
      allFromAnother: Boolean = false,
      method: String = "AUTO"): H2OFrame = {
    val endpoint = getClusterEndpoint(conf)
    val params = Map(
      "ast" -> MessageFormat.format(
        "( assign {0} (merge {1} {2} {3} {4} [] [] \"{5}\"))",
        s"${this.frameId}_join_${Random.alphanumeric.take(5).mkString("")}",
        this.frameId,
        another.frameId,
        if (allFromCurrent) "1" else "0",
        if (allFromAnother) "1" else "0",
        method.toLowerCase()))
    val rapidsFrameV3 = update[RapidsFrameV3](endpoint, "99/Rapids", conf, params)
    H2OFrame(rapidsFrameV3.key.name)
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
    val parameters = Map("key" -> frameId, "columns" -> columns)
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
