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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.h2o.converters.WriteConverterCtx
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.h2o.utils.{NodeDesc, ReflectionUtils}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.types._
import water._
import ai.h2o.sparkling.extensions.serde.ChunkAutoBufferWriter

class ExternalWriteConverterCtx(conf: H2OConf, nodeDesc: NodeDesc)
  extends WriteConverterCtx {

  private var expectedTypes: Array[Byte] = null
  private var currentColIdx: Int = 0
  private var currentRowIdx: Int = 0
  private var chunkWriter: ChunkAutoBufferWriter = null

  override def closeChunks(numRows: Int): Unit = {
    chunkWriter.close()
  }

  override def initFrame(key: String, columns: Array[String]): Unit = {
    ???
  }

  override def finalizeFrame(key: String, rowsPerChunk: Array[Long], colTypes: Array[Byte], domains: Array[Array[String]] = null): Unit = {
    ???
  }

  /**
    * Initialize the communication before the chunks are created
    */
  override def createChunk(frameName: String, numRows: Option[Int], expectedTypes: Array[Byte], chunkId: Int, maxVecSizes: Array[Int],
                           sparse: Array[Boolean], vecStartSize: Map[Int, Int]): Unit = {
    this.expectedTypes = expectedTypes
    val outputStream = RestApiUtils.putChunk(nodeDesc, conf, frameName, chunkId, expectedTypes, maxVecSizes)
    this.chunkWriter = new ChunkAutoBufferWriter(outputStream)
  }

  override def put(colIdx: Int, data: Boolean): Unit = {
    chunkWriter.writeBoolean(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Byte): Unit = {
    chunkWriter.writeByte(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Char): Unit = {
    chunkWriter.writeChar(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Short): Unit = {
    chunkWriter.writeShort(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Int): Unit = {
    chunkWriter.writeInt(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Long): Unit = {
    chunkWriter.writeLong(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Float): Unit = {
    chunkWriter.writeFloat(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: Double): Unit = {
    chunkWriter.writeDouble(data)
    increaseCounters()
  }

  override def put(colIdx: Int, data: java.sql.Timestamp): Unit = {
    chunkWriter.writeTimestamp(data)
    increaseCounters()
  }

  // Here we should call externalFrameWriter.sendDate - however such method does not exist
  // Hence going through the same path as sending Timestamp long value.
  override def put(colIdx: Int, data: java.sql.Date): Unit = {
    chunkWriter.writeLong(data.getTime)
    increaseCounters()
  }

  override def put(colIdx: Int, data: String): Unit = {
    chunkWriter.writeString(data)
    increaseCounters()
  }

  override def putNA(columnNum: Int): Unit = {
    chunkWriter.writeNA(expectedTypes(columnNum))
    increaseCounters()
  }

  override def putSparseVector(startIdx: Int, vector: SparseVector, maxVecSize: Int): Unit = {
    chunkWriter.writeSparseVector(vector.indices, vector.values)
    increaseCounters()
  }

  override def putDenseVector(startIdx: Int, vector: DenseVector, maxVecSize: Int): Unit = {
    chunkWriter.writeDenseVector(vector.values)
    increaseCounters()
  }

  override def numOfRows(): Int = currentRowIdx

  override def startRow(rowIdx: Int): Unit = {}

  override def finishRow(): Unit = {}

  private def increaseCounters(): Unit = {
    currentColIdx += 1
    if (currentColIdx == expectedTypes.length) {
      currentRowIdx += 1
      currentColIdx = 0
    }
  }
}

object ExternalWriteConverterCtx {

  import scala.language.postfixOps

  def scheduleUpload(numPartitions: Int): UploadPlan = {
    val nodes = H2OContext.ensure("H2OContext needs to be running").getH2ONodes()
    val uploadPlan = (0 until numPartitions).zip(Stream.continually(nodes).flatten).toMap
    uploadPlan
  }

  def internalJavaClassOf(dt: DataType): Class[_] = {
    dt match {
      case n if n.isInstanceOf[DecimalType] & n.getClass.getSuperclass != classOf[DecimalType] => Double.javaClass
      case _: DateType => Long.javaClass
      case _: UserDefinedType[_ /*ml.linalg.Vector */ ] => classOf[Vector]
      case _: DataType => ReflectionUtils.supportedTypeOf(dt).javaClass
    }
  }
}
