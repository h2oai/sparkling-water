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

import org.apache.spark._
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.backends.external.ExternalWriteConverterCtx
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.{H2OSchemaUtils, ReflectionUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, H2OFrameRelation, Row, SQLContext}
import water.fvec.{Frame, H2OFrame}
import water.{ExternalFrameUtils, Key}


private[h2o] object SparkDataFrameConverter extends Logging {

  /**
    * Create a Spark DataFrame from given H2O frame.
    *
    * @param hc an instance of H2O context
    * @param fr  an instance of H2O frame
    * @param copyMetadata  copy H2O metadata into Spark DataFrame
    * @param sqlContext  running sqlContext
    * @tparam T  type of H2O frame
    * @return  a new DataFrame definition using given H2OFrame as data source
    */

  def toDataFrame[T <: Frame](hc: H2OContext, fr: T, copyMetadata: Boolean)(implicit sqlContext: SQLContext): DataFrame = {
    // Relation referencing H2OFrame
    val relation = H2OFrameRelation(fr, copyMetadata)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  /** Transform Spark's DataFrame into H2O Frame */
  def toH2OFrame(hc: H2OContext, dataFrame: DataFrame, frameKeyName: Option[String]): H2OFrame = {
    import H2OSchemaUtils._
    // Cache DataFrame RDD's
    val dfRdd = dataFrame.rdd
    val keyName = frameKeyName.getOrElse("frame_rdd_" + dfRdd.id + Key.rand())

    // Flattens and expands RDD's schema
    val flatRddSchema = expandedSchema(hc.sparkContext, dataFrame)
    // Patch the flat schema based on information about types
    val fnames = flatRddSchema.map(t => t._2.name).toArray

    // in case of internal backend, store regular vector types
    // otherwise for external backend store expected types
    val expectedTypes = if(hc.getConf.runsInInternalClusterMode){
      // Transform datatype into h2o types
      flatRddSchema.map(f => ReflectionUtils.vecTypeFor(f._2.dataType)).toArray
    }else{
      val internalJavaClasses = flatRddSchema.map{f =>
        ExternalWriteConverterCtx.internalJavaClassOf(f._2.dataType)
      }.toArray
      ExternalFrameUtils.prepareExpectedTypes(internalJavaClasses)
    }
    WriteConverterCtxUtils.convert[Row](hc, dfRdd, keyName, fnames, expectedTypes, perSQLPartition(flatRddSchema))
  }

  /**
    *
    * @param keyName    key of the frame
    * @param vecTypes   h2o vec types
    * @param types      flat RDD schema
    * @param uploadPlan plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context    spark task context
    * @param it         iterator over data in the partition
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perSQLPartition(types: Seq[(Seq[Int], StructField, Byte)])
                     (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[UploadPlan], writeTimeout: Int)
                     (context: TaskContext, it: Iterator[Row]): (Int, Long) = {

    val (iterator, dataSize) = WriteConverterCtxUtils.bufferedIteratorWithSize(uploadPlan, it)
    val con = WriteConverterCtxUtils.create(uploadPlan, context.partitionId(), dataSize, writeTimeout)
    // Creates array of H2O NewChunks; A place to record all the data in this partition
    con.createChunks(keyName, vecTypes, context.partitionId())

    iterator.foreach(row => {
      var startOfSeq = -1
      // Fill row in the output frame
      types.indices.foreach { idx => // Index of column
        val field = types(idx)
        val path = field._1
        val dataType = field._2.dataType
        // Helpers to distinguish embedded collection types
        val isAry = field._3 == H2OSchemaUtils.ARRAY_TYPE
        val isVec = field._3 == H2OSchemaUtils.VEC_TYPE
        val isNewPath = if (idx > 0) path != types(idx - 1)._1 else true
        // Reset counter for sequences
        if ((isAry || isVec) && isNewPath) startOfSeq = idx
        else if (!isAry && !isVec) startOfSeq = -1

        var i = 0
        var subRow = row
        while (i < path.length - 1 && !subRow.isNullAt(path(i))) {
          subRow = subRow.getAs[Row](path(i))
          i += 1
        }
        val aidx = path(i) // actual index into row provided by path
        if (subRow.isNullAt(aidx)) {
          con.putNA(idx)
        } else {
          val ary = if (isAry) subRow.getAs[Seq[_]](aidx) else null
          val aryLen = if (isAry) ary.length else -1
          val aryIdx = idx - startOfSeq // shared index to position in array/vector
          val vecLen = if (isVec) getVecLen(subRow, aidx) else -1
          if (isAry && aryIdx >= aryLen) con.putNA(idx)
          else if (isVec && aryIdx >= vecLen) con.put(idx, 0.0) // Add zeros for double vectors
          else dataType match {
            case BooleanType => con.put(idx, if (isAry)
              if (ary(aryIdx).asInstanceOf[Boolean]) 1 else 0
            else if (subRow.getBoolean(aidx)) 1 else 0)
            case BinaryType =>
            case ByteType => con.put(idx, if (isAry) ary(aryIdx).asInstanceOf[Byte] else subRow.getByte(aidx))
            case ShortType => con.put(idx, if (isAry) ary(aryIdx).asInstanceOf[Short] else subRow.getShort(aidx))
            case IntegerType => con.put(idx, if (isAry) ary(aryIdx).asInstanceOf[Int] else subRow.getInt(aidx))
            case LongType => con.put(idx, if (isAry) ary(aryIdx).asInstanceOf[Long] else subRow.getLong(aidx))
            case FloatType => con.put(idx, if (isAry) ary(aryIdx).asInstanceOf[Float] else subRow.getFloat(aidx))
            case _: DecimalType => con.put(idx, if (isAry) {
              ary(aryIdx).asInstanceOf[BigDecimal].doubleValue()
            } else {
              if (isVec) {
                getVecVal(subRow, aidx, idx - startOfSeq)
              } else {
                subRow.getDecimal(aidx).doubleValue()
              }
            })
            case DoubleType => con.put(idx, if (isAry) {
              ary(aryIdx).asInstanceOf[Double]
            } else {
              if (isVec) {
                getVecVal(subRow, aidx, idx - startOfSeq)
              } else {
                subRow.getDouble(aidx)
              }
            })
            case StringType =>
              val sv = if (isAry) ary(aryIdx).asInstanceOf[String] else subRow.getString(aidx)
              // Always produce string vectors
              con.put(idx, sv)

            case TimestampType => con.put(idx, subRow.getAs[java.sql.Timestamp](aidx))
            case DateType      => con.put(idx, subRow.getAs[java.sql.Date](aidx))
            case _ => con.putNA(idx)
          }
        }

      }
    })

    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows())
  }

  private def getVecLen(r: Row, idx: Int): Int = {
    val value = r.get(idx)
    value match {
      case vector: mllib.linalg.Vector =>
        vector.size
      case vector: ml.linalg.Vector =>
        vector.size
      case _ =>
        -1
    }
  }

  private def getVecVal(r: Row, ridx: Int, vidx: Int): Double = {
    val value = r.get(ridx)
    value match {
      case vector: mllib.linalg.Vector =>
        vector(vidx)
      case vector: ml.linalg.Vector =>
        vector(vidx)
      case _ =>
        throw new ArrayIndexOutOfBoundsException(s"Row: ${r}, row index: ${ridx}, vector index: ${vidx}")
    }
  }
}
