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

package org.apache.spark.h2o.converters

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.backends.external.{ExternalBackendUtils, ExternalWriteConverterCtx}
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.{H2OSchemaUtils, ReflectionUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType, _}
import org.apache.spark.sql.{DataFrame, H2OFrameRelation, Row, SQLContext}
import org.apache.spark.{mllib, _}
import water.Key
import water.fvec.{Frame, H2OFrame}


private[h2o] object SparkDataFrameConverter extends Logging {

  /**
    * Create a Spark DataFrame from given H2O frame.
    *
    * @param hc           an instance of H2O context
    * @param fr           an instance of H2O frame
    * @param copyMetadata copy H2O metadata into Spark DataFrame
    * @param sqlContext   running sqlContext
    * @tparam T type of H2O frame
    * @return a new DataFrame definition using given H2OFrame as data source
    */

  def toDataFrame[T <: Frame](hc: H2OContext, fr: T, copyMetadata: Boolean)(implicit sqlContext: SQLContext): DataFrame = {
    // Relation referencing H2OFrame
    val relation = H2OFrameRelation(fr, copyMetadata)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }


  /** Transform Spark's DataFrame into H2O Frame */
  def toH2OFrame(hc: H2OContext, dataFrame: DataFrame, frameKeyName: Option[String]): H2OFrame = {
    import H2OSchemaUtils._

    // Flatten the dataframe so we don't have any nested rows
    val flatDataFrame = flattenDataFrame(dataFrame)

    val dfRdd = flatDataFrame.rdd
    val keyName = frameKeyName.getOrElse("frame_rdd_" + dfRdd.id + Key.rand())
    val elemMaxSizes = collectMaxElementSizes(hc.sparkContext, flatDataFrame)
    val vecMaxSizes = collectVectorLikeTypes(flatDataFrame.schema).map(elemMaxSizes(_)).toArray
    val startPositions = collectElemStartPositions(elemMaxSizes)

    // Expands RDD's schema ( Arrays and Vectors)
    val flatRddSchema = expandedSchema(hc.sparkContext, H2OSchemaUtils.flattenSchema(dataFrame.schema), elemMaxSizes)
    // Patch the flat schema based on information about types
    val fnames = flatRddSchema.map(_.name).toArray

    // in case of internal backend, store regular vector types
    // otherwise for external backend store expected types
    val expectedTypes = if (hc.getConf.runsInInternalClusterMode) {
      // Transform datatype into h2o types
      flatRddSchema.map(f => ReflectionUtils.vecTypeFor(f.dataType)).toArray
    } else {
      val internalJavaClasses = H2OSchemaUtils.expandWithoutVectors(hc.sparkContext, H2OSchemaUtils.flattenSchema(dataFrame.schema), elemMaxSizes).map { f =>
        ExternalWriteConverterCtx.internalJavaClassOf(f.dataType)
      }.toArray
      ExternalBackendUtils.prepareExpectedTypes(internalJavaClasses)
    }

    WriteConverterCtxUtils.convert[Row](hc, dfRdd, keyName, fnames, expectedTypes, vecMaxSizes,
      perSQLPartition(flatRddSchema, elemMaxSizes, vecMaxSizes, startPositions))
  }

  /**
    *
    * @param keyName        key of the frame
    * @param vecTypes       h2o vec types
    * @param types          flat RDD schema
    * @param elemMaxSizes   array containing max size of each element in the dataframe
    * @param startPositions array containing positions in h2o frame corresponding to spark frame
    * @param uploadPlan     plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context        spark task context
    * @param it             iterator over data in the partition
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perSQLPartition(types: Seq[StructField], elemMaxSizes: Array[Int], vecMaxSizes: Array[Int], startPositions: Array[Int])
                     (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[UploadPlan], writeTimeout: Int)
                     (context: TaskContext, it: Iterator[Row]): (Int, Long) = {

    val (iterator, dataSize) = WriteConverterCtxUtils.bufferedIteratorWithSize(uploadPlan, it)
    val con = WriteConverterCtxUtils.create(uploadPlan, context.partitionId(), dataSize, writeTimeout)
    // Creates array of H2O NewChunks; A place to record all the data in this partition
    con.createChunks(keyName, vecTypes, context.partitionId(), vecMaxSizes)

    iterator.foreach {
      sparkRowToH2ORow(_, con, startPositions, elemMaxSizes)
    }

    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows())
  }

  /**
    * Converts a single Spark Row to H2O Row with expanded vectors and arrays
    */
  def sparkRowToH2ORow(row: Row, con: WriteConverterCtx, startIndices: Array[Int], elemSizes: Array[Int]) {
    row.schema.fields.zipWithIndex.foreach { case (entry, idxRow) =>
      val idxH2O = startIndices(idxRow)
      if (row.isNullAt(idxRow)) {
        con.putNA(idxH2O)
      } else {
        entry.dataType match {
          case BooleanType => con.put(idxH2O, if (row.getBoolean(idxRow)) 1 else 0)
          case BinaryType =>
          case ByteType => con.put(idxH2O, row.getByte(idxRow))
          case ShortType => con.put(idxH2O, row.getShort(idxRow))
          case IntegerType => con.put(idxH2O, row.getInt(idxRow))
          case LongType => con.put(idxH2O, row.getLong(idxRow))
          case FloatType => con.put(idxH2O, row.getFloat(idxRow))
          case _: DecimalType => con.put(idxH2O, row.getDecimal(idxRow).doubleValue())
          case DoubleType => con.put(idxH2O, row.getDouble(idxRow))
          case StringType => con.put(idxH2O, row.getString(idxRow))
          case TimestampType => con.put(idxH2O, row.getAs[java.sql.Timestamp](idxRow))
          case DateType => con.put(idxH2O, row.getAs[java.sql.Date](idxRow))
          case ArrayType(elemType, _) => putArray(row.getAs[Seq[_]](idxRow), elemType, con, idxH2O, elemSizes(idxRow))
          case _: UserDefinedType[_ /*mllib.linalg.Vector*/ ] => {
            val value = row.get(idxRow)
            value match {
              case vector: mllib.linalg.Vector =>
                con.putVector(idxH2O, vector, elemSizes(idxH2O))
              case vector: ml.linalg.Vector =>
                con.putVector(idxH2O, vector, elemSizes(idxH2O))
            }
          }
          case _ => con.putNA(idxH2O)
        }
      }
    }
  }

  private def putArray(arr: Seq[_], elemType: DataType, con: WriteConverterCtx, idx: Int, maxArrSize: Int) {
    arr.indices.foreach { arrIdx =>
      val currentIdx = idx + arrIdx
      elemType match {
        case BooleanType => con.put(currentIdx, if (arr(arrIdx).asInstanceOf[Boolean]) 1 else 0)
        case BinaryType =>
        case ByteType => con.put(currentIdx, arr(arrIdx).asInstanceOf[Byte])
        case ShortType => con.put(currentIdx, arr(arrIdx).asInstanceOf[Short])
        case IntegerType => con.put(currentIdx, arr(arrIdx).asInstanceOf[Int])
        case LongType => con.put(currentIdx, arr(arrIdx).asInstanceOf[Long])
        case FloatType => con.put(currentIdx, arr(arrIdx).asInstanceOf[Float])
        case _: DecimalType => con.put(currentIdx, arr(arrIdx).asInstanceOf[BigDecimal].doubleValue())
        case DoubleType => con.put(currentIdx, arr(arrIdx).asInstanceOf[Double])
        case StringType => con.put(currentIdx, arr(arrIdx).asInstanceOf[String])
        case TimestampType => con.put(currentIdx, arr(arrIdx).asInstanceOf[java.sql.Timestamp])
        case DateType => con.put(currentIdx, arr(arrIdx).asInstanceOf[java.sql.Date])
        case _ => con.putNA(currentIdx)
      }
    }

    (arr.size until maxArrSize).foreach { arrIdx => con.putNA(idx + arrIdx) }
  }
}
