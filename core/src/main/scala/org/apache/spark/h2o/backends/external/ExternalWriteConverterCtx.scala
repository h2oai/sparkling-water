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
import ai.h2o.sparkling.extensions.serde.ChunkAutoBufferWriter
import ai.h2o.sparkling.frame.{H2OChunk, H2OFrame}

class ExternalWriteConverterCtx(conf: H2OConf, nodeDesc: NodeDesc)
  extends WriteConverterCtx {

  private var expectedTypes: Array[Byte] = null
  private var chunkWriter: ChunkAutoBufferWriter = null
  private var numberOfRows: Int = -1

  override def closeChunks(numRows: Int): Unit = {
    chunkWriter.close()
  }

  override def initFrame(key: String, columns: Array[String]): Unit = {
    H2OFrame.initializeFrame(conf, key, columns)
  }

  override def finalizeFrame(key: String, rowsPerChunk: Array[Long], colTypes: Array[Byte]): Unit = {
    H2OFrame.finalizeFrame(conf, key, rowsPerChunk, colTypes)
  }

  /**
    * Initialize the communication before the chunks are created
    */
  override def createChunk(frameName: String, numRows: Int, expectedTypes: Array[Byte], chunkId: Int, maxVecSizes: Array[Int],
                           sparse: Array[Boolean], vecStartSize: Map[Int, Int]): Unit = {
    this.expectedTypes = expectedTypes
    val outputStream = H2OChunk.putChunk(nodeDesc, conf, frameName, numRows, chunkId, expectedTypes, maxVecSizes)
    this.chunkWriter = new ChunkAutoBufferWriter(outputStream)
    this.numberOfRows = numRows
  }

  override def put(colIdx: Int, data: Boolean): Unit = chunkWriter.writeBoolean(data)

  override def put(colIdx: Int, data: Byte): Unit = chunkWriter.writeByte(data)

  override def put(colIdx: Int, data: Char): Unit = chunkWriter.writeChar(data)

  override def put(colIdx: Int, data: Short): Unit = chunkWriter.writeShort(data)

  override def put(colIdx: Int, data: Int): Unit = chunkWriter.writeInt(data)

  override def put(colIdx: Int, data: Long): Unit = chunkWriter.writeLong(data)

  override def put(colIdx: Int, data: Float): Unit = chunkWriter.writeFloat(data)

  override def put(colIdx: Int, data: Double): Unit = chunkWriter.writeDouble(data)

  override def put(colIdx: Int, data: java.sql.Timestamp): Unit = chunkWriter.writeTimestamp(data)

  // Here we should call externalFrameWriter.sendDate - however such method does not exist
  // Hence going through the same path as sending Timestamp long value.
  override def put(colIdx: Int, data: java.sql.Date): Unit = chunkWriter.writeLong(data.getTime)

  override def put(colIdx: Int, data: String): Unit = chunkWriter.writeString(data)

  override def putNA(columnIdx: Int, sparkIdx: Int): Unit = chunkWriter.writeNA(expectedTypes(sparkIdx))

  override def numOfRows(): Int = this.numberOfRows

  override def putSparseVector(startIdx: Int, vector: SparseVector, maxVecSize: Int): Unit = {
    chunkWriter.writeSparseVector(vector.indices, vector.values)
  }

  override def putDenseVector(startIdx: Int, vector: DenseVector, maxVecSize: Int): Unit = {
    chunkWriter.writeDenseVector(vector.values)
  }

  override def startRow(rowIdx: Int): Unit = {}

  override def finishRow(): Unit = {}
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
