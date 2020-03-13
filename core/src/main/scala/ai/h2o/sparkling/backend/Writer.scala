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

package ai.h2o.sparkling.backend

import java.io.Closeable

import ai.h2o.sparkling.extensions.serde.{ChunkAutoBufferWriter, SerdeUtils}
import ai.h2o.sparkling.frame.{H2OChunk, H2OFrame}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OContext, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{ExposeUtils, TaskContext, ml, mllib}
import water.H2O

private[backend] class Writer(nodeDesc: NodeDesc,
                              metadata: WriterMetadata,
                              numRows: Int,
                              chunkId: Int) extends Closeable {

  private val outputStream = H2OChunk.putChunk(nodeDesc,
    metadata.conf,
    metadata.frameId,
    numRows,
    chunkId,
    metadata.expectedTypes,
    metadata.maxVectorSizes)

  private val chunkWriter = new ChunkAutoBufferWriter(outputStream)

  def put(data: Boolean): Unit = chunkWriter.writeBoolean(data)

  def put(data: Byte): Unit = chunkWriter.writeByte(data)

  def put(data: Char): Unit = chunkWriter.writeChar(data)

  def put(data: Short): Unit = chunkWriter.writeShort(data)

  def put(data: Int): Unit = chunkWriter.writeInt(data)

  def put(data: Long): Unit = chunkWriter.writeLong(data)

  def put(data: Float): Unit = chunkWriter.writeFloat(data)

  def put(data: Double): Unit = chunkWriter.writeDouble(data)

  def put(data: java.sql.Timestamp): Unit = chunkWriter.writeTimestamp(data)

  def put(data: java.sql.Date): Unit = chunkWriter.writeLong(data.getTime)

  def put(data: String): Unit = chunkWriter.writeString(data)

  def putNA(sparkIdx: Int): Unit = chunkWriter.writeNA(metadata.expectedTypes(sparkIdx))

  def putSparseVector(vector: ml.linalg.SparseVector): Unit = chunkWriter.writeSparseVector(vector.indices, vector.values)

  def putDenseVector(vector: ml.linalg.DenseVector): Unit = chunkWriter.writeDenseVector(vector.values)

  def putVector(vector: mllib.linalg.Vector): Unit = putVector(vector.asML)

  def putVector(vector: ml.linalg.Vector): Unit = {
    vector match {
      case sparseVector: ml.linalg.SparseVector =>
        putSparseVector(sparseVector)
      case denseVector: ml.linalg.DenseVector =>
        putDenseVector(denseVector)
    }
  }

  def close(): Unit = chunkWriter.close()
}

private[backend] object Writer {

  type SparkJob = (TaskContext, Iterator[Row]) => (Int, Long)
  type UploadPlan = Map[Int, NodeDesc]

  def convert(rdd: H2OAwareRDD[Row], colNames: Array[String], metadata: WriterMetadata): String = {
    H2OFrame.initializeFrame(metadata.conf, metadata.frameId, colNames)
    val partitionSizes = getNonEmptyPartitionSizes(rdd)
    val nonEmptyPartitions = getNonEmptyPartitions(partitionSizes)

    val uploadPlan = scheduleUpload(nonEmptyPartitions.size, rdd)
    val operation: SparkJob = perDataFramePartition(metadata, uploadPlan, nonEmptyPartitions, partitionSizes)
    val rows = SparkSessionUtils.active.sparkContext.runJob(rdd, operation, nonEmptyPartitions)
    val res = new Array[Long](nonEmptyPartitions.size)
    rows.foreach { case (chunkIdx, numRows) => res(chunkIdx) = numRows }
    val types = SerdeUtils.expectedTypesToVecTypes(metadata.expectedTypes, metadata.maxVectorSizes)
    H2OFrame.finalizeFrame(metadata.conf, metadata.frameId, res, types)
    metadata.frameId
  }

  private def perDataFramePartition(metadata: WriterMetadata,
                                    uploadPlan: UploadPlan,
                                    partitions: Seq[Int],
                                    partitionSizes: Map[Int, Int])(context: TaskContext, it: Iterator[Row]): (Int, Long) = {
    val chunkIdx = partitions.indexOf(context.partitionId())
    val numRows = partitionSizes(context.partitionId())
    withResource(new Writer(uploadPlan(chunkIdx), metadata, numRows, chunkIdx)) { writer =>
      it.foreach { row => sparkRowToH2ORow(row, writer) }
    }
    (chunkIdx, numRows)
  }

  private def sparkRowToH2ORow(row: Row, con: Writer): Unit = {
    row.schema.fields.zipWithIndex.foreach { case (entry, idxField) =>
      if (row.isNullAt(idxField)) {
        con.putNA(idxField)
      } else {
        entry.dataType match {
          case BooleanType => con.put(row.getBoolean(idxField))
          case ByteType => con.put(row.getByte(idxField))
          case ShortType => con.put(row.getShort(idxField))
          case IntegerType => con.put(row.getInt(idxField))
          case LongType => con.put(row.getLong(idxField))
          case FloatType => con.put(row.getFloat(idxField))
          case _: DecimalType => con.put(row.getDecimal(idxField).doubleValue())
          case DoubleType => con.put(row.getDouble(idxField))
          case StringType => con.put(row.getString(idxField))
          case TimestampType => con.put(row.getAs[java.sql.Timestamp](idxField))
          case DateType => con.put(row.getAs[java.sql.Date](idxField))
          case v if ExposeUtils.isMLVectorUDT(v) => con.putVector(row.getAs[ml.linalg.Vector](idxField))
          case _: mllib.linalg.VectorUDT => con.putVector(row.getAs[mllib.linalg.Vector](idxField))
          case udt if ExposeUtils.isUDT(udt) => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
          case unsupported => throw new UnsupportedOperationException(s"Data of type ${unsupported.getClass} are not supported for the conversion" +
            s"to H2OFrame.")
        }
      }
    }
  }

  private def scheduleUpload[T](numPartitions: Int, rdd: H2OAwareRDD[T]): UploadPlan = {
    val hc = H2OContext.ensure("H2OContext needs to be running")
    val nodes = hc.getH2ONodes()
    if (hc.getConf.runsInInternalClusterMode) {
      rdd.mapPartitionsWithIndex { case (idx, _) =>
        Iterator.single((idx, NodeDesc(H2O.SELF)))
      }.collect().toMap
    } else {
      val uploadPlan = (0 until numPartitions).zip(Stream.continually(nodes).flatten).toMap
      uploadPlan
    }
  }

  private def getNonEmptyPartitionSizes[T](rdd: RDD[T]): Map[Int, Int] = {
    rdd.mapPartitionsWithIndex {
      case (idx, it) => if (it.nonEmpty) {
        Iterator.single((idx, it.size))
      } else {
        Iterator.empty
      }
    }.collect().toMap
  }

  private def getNonEmptyPartitions(partitionSizes: Map[Int, Int]): Seq[Int] = {
    partitionSizes.keys.toSeq.sorted
  }
}
