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
    // Flatten the Spark data frame so we don't have any nested rows
    val flatDataFrame = flattenDataFrame(dataFrame)
    val dfRdd = flatDataFrame.rdd
    val keyName = frameKeyName.getOrElse("frame_rdd_" + dfRdd.id + Key.rand())

    val elemMaxSizes = collectMaxElementSizes(hc.sparkContext, flatDataFrame)
    val elemStartIndices = collectElemStartPositions(elemMaxSizes)
    val vecIndices = collectVectorLikeTypes(flatDataFrame.schema).toArray

    // Expands RDD's schema ( Arrays and Vectors)
    val flatRddSchema = expandedSchema(hc.sparkContext, flatDataFrame.schema, elemMaxSizes)
    // Patch the flat schema based on information about types
    val fnames = flatRddSchema.map(_.name).toArray

    // In case of internal backend, store regular H2O vector types
    // otherwise for external backend, store expected types
    val expectedTypes = if (hc.getConf.runsInInternalClusterMode) {
      // Transform datatype into h2o types
      flatRddSchema.map(f => ReflectionUtils.vecTypeFor(f.dataType)).toArray
    } else {
      val internalJavaClasses = H2OSchemaUtils.expandWithoutVectors(hc.sparkContext, flatDataFrame.schema, elemMaxSizes).map { f =>
        ExternalWriteConverterCtx.internalJavaClassOf(f.dataType)
      }.toArray
      ExternalBackendUtils.prepareExpectedTypes(internalJavaClasses)
    }

    WriteConverterCtxUtils.convert[Row](hc, dfRdd, keyName, fnames, expectedTypes, vecIndices.map(elemMaxSizes(_)),
      perSQLPartition(elemMaxSizes, elemStartIndices, vecIndices))
  }

  /**
    *
    * @param keyName        key of the frame
    * @param expectedTypes  expected types of H2O vectors after the corresponding data are converted from Spark
    * @param elemMaxSizes   array containing max size of each element in the dataframe
    * @param elemStartIndices array containing positions in h2o frame corresponding to spark frame
    * @param uploadPlan     plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context        spark task context
    * @param it             iterator over data in the partition
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perSQLPartition(elemMaxSizes: Array[Int], elemStartIndices: Array[Int], vecIndices: Array[Int])
                     (keyName: String, expectedTypes: Array[Byte], uploadPlan: Option[UploadPlan], writeTimeout: Int)
                     (context: TaskContext, it: Iterator[Row]): (Int, Long) = {

    val (iterator, dataSize) = WriteConverterCtxUtils.bufferedIteratorWithSize(uploadPlan, it)
    val con = WriteConverterCtxUtils.create(uploadPlan, context.partitionId(), dataSize, writeTimeout)
    // Collect mapping start position of vector and its size
    val vecStartSize = (for (vecIdx <- vecIndices) yield {
      (elemStartIndices(vecIdx), elemMaxSizes(vecIdx))
    }).toMap
    // Creates array of H2O NewChunks; A place to record all the data in this partition
    con.createChunks(keyName, expectedTypes, context.partitionId(), vecIndices.map(elemMaxSizes(_)), vecStartSize)

    var localRowIdx = 0
    iterator.foreach { row =>
      sparkRowToH2ORow(row, localRowIdx, con, elemStartIndices, elemMaxSizes)
      localRowIdx += 1
    }

    //Compress & write data in partitions to H2O Chunks
    con.closeChunks(localRowIdx)

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows())
  }

  /**
    * Converts a single Spark Row to H2O Row with expanded vectors and arrays
    */
  def sparkRowToH2ORow(row: Row, rowIdx: Int, con: WriteConverterCtx, elemStartIndices: Array[Int], elemSizes: Array[Int]) {
    con.startRow(rowIdx)
    row.schema.fields.zipWithIndex.foreach { case (entry, idxField) =>
      val idxH2O = elemStartIndices(idxField)
      if (row.isNullAt(idxField)) {
        con.putNA(idxH2O)
      } else {
        entry.dataType match {
          case BooleanType => con.put(idxH2O, if (row.getBoolean(idxField)) 1 else 0)
          case BinaryType => putArray(row.getAs[Array[Byte]](idxField), ByteType, con, idxH2O, elemSizes(idxField))
          case ByteType => con.put(idxH2O, row.getByte(idxField))
          case ShortType => con.put(idxH2O, row.getShort(idxField))
          case IntegerType => con.put(idxH2O, row.getInt(idxField))
          case LongType => con.put(idxH2O, row.getLong(idxField))
          case FloatType => con.put(idxH2O, row.getFloat(idxField))
          case _: DecimalType => con.put(idxH2O, row.getDecimal(idxField).doubleValue())
          case DoubleType => con.put(idxH2O, row.getDouble(idxField))
          case StringType => con.put(idxH2O, row.getString(idxField))
          case TimestampType => con.put(idxH2O, row.getAs[java.sql.Timestamp](idxField))
          case DateType => con.put(idxH2O, row.getAs[java.sql.Date](idxField))
          case ArrayType(elemType, _) => putArray(row.getAs[Seq[_]](idxField), elemType, con, idxH2O, elemSizes(idxField))
          case _: UserDefinedType[_ /*mllib.linalg.Vector*/ ] => {
            val value = row.get(idxField)
            value match {
              case vector: mllib.linalg.Vector =>
                con.putVector(idxH2O, vector, elemSizes(idxField))
              case vector: ml.linalg.Vector =>
                con.putVector(idxH2O, vector, elemSizes(idxField))
            }
          }
          case _ => con.putNA(idxH2O)
        }
      }
    }
    con.finishRow()
  }

  private def putArray(arr: Seq[_], elemType: DataType, con: WriteConverterCtx, idx: Int, maxArrSize: Int) {
    arr.indices.foreach { arrIdx =>
      val currentIdx = idx + arrIdx
      elemType match {
        case BooleanType => con.put(currentIdx, if (arr(arrIdx).asInstanceOf[Boolean]) 1 else 0)
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
