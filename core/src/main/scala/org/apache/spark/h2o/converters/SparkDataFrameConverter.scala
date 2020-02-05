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

import org.apache.spark.h2o.{H2OConf, H2OContext, H2OFrame}
import org.apache.spark.h2o.backends.external.{ExternalH2OBackend, ExternalWriteConverterCtx}
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType, _}
import org.apache.spark.sql.{DataFrame, H2OFrameRelation, H2ORESTFrameRelation, Row}
import org.apache.spark.{mllib, _}
import water.{DKV, Key}
import water.fvec.{Frame, H2OFrame}


private[h2o] object SparkDataFrameConverter extends Logging {

  /**
    * Create a Spark DataFrame from given H2O frame.
    *
    * @param hc           an instance of H2O context
    * @param fr           an instance of H2O frame
    * @param copyMetadata copy H2O metadata into Spark DataFrame
    * @tparam T type of H2O frame
    * @return a new DataFrame definition using given H2OFrame as data source
    */

  def toDataFrame[T <: Frame](hc: H2OContext, fr: T, copyMetadata: Boolean): DataFrame = {
    // Relation referencing H2OFrame
    val relation = H2OFrameRelation(fr, copyMetadata)(hc.sparkSession.sqlContext)
    hc.sparkSession.sqlContext.baseRelationToDataFrame(relation)
  }

  /**
    * Create a Spark DataFrame from a given REST-based H2O frame.
    *
    * @param hc           an instance of H2O context
    * @param fr           an instance of H2O frame
    * @param copyMetadata copy H2O metadata into Spark DataFrame

    * @return a new DataFrame definition using given H2OFrame as data source
    */

  def toDataFrame(hc: H2OContext, fr: ai.h2o.sparkling.frame.H2OFrame, copyMetadata: Boolean): DataFrame = {
    // Relation referencing H2OFrame
    val relation = H2ORESTFrameRelation(fr, copyMetadata)(hc.sparkSession.sqlContext)
    hc.sparkSession.sqlContext.baseRelationToDataFrame(relation)
  }

  /** Transform Spark's DataFrame into H2O Frame */
  def toH2OFrame(hc: H2OContext, dataFrame: DataFrame, frameKeyName: Option[String]): H2OFrame = {
    val key = toH2OFrameKeyString(hc, dataFrame, frameKeyName, WriteConverterCtxUtils.ClientBasedConverter)
    new H2OFrame(DKV.getGet[Frame](key))
  }

  def toH2OFrameKeyString(hc: H2OContext, dataFrame: DataFrame, frameKeyName: Option[String], converter: WriteConverterCtxUtils.Converter): String = {
    import ai.h2o.sparkling.ml.utils.SchemaUtils._

    val flatDataFrame = flattenDataFrame(dataFrame)
    val dfRdd = flatDataFrame.rdd
    val keyName = frameKeyName.getOrElse("frame_rdd_" + dfRdd.id + Key.rand())

    val elemMaxSizes = collectMaxElementSizes(flatDataFrame)
    val elemStartIndices = collectElemStartPositions(elemMaxSizes)
    val vecIndices = collectVectorLikeTypes(flatDataFrame.schema).toArray
    val sparseInfo = collectSparseInfo(flatDataFrame, elemMaxSizes)
    // Expands RDD's schema ( Arrays and Vectors)
    val flatRddSchema = expandedSchema(flatDataFrame.schema, elemMaxSizes)
    // Patch the flat schema based on information about types
    val fnames = flatRddSchema.map(_.name).toArray

    // In case of internal backend, store regular H2O vector types
    // otherwise for external backend, store expected types
    val expectedTypes = if (hc.getConf.runsInInternalClusterMode) {
      // Transform datatype into h2o types
      flatRddSchema.map(f => ReflectionUtils.vecTypeFor(f.dataType)).toArray
    } else {
      val internalJavaClasses = flatDataFrame.schema.map { f =>
        ExternalWriteConverterCtx.internalJavaClassOf(f.dataType)
      }.toArray
      ExternalH2OBackend.prepareExpectedTypes(internalJavaClasses)
    }

    val blockSize = hc.getConf.externalCommunicationBlockSizeAsBytes
    converter.convert[Row](hc, dfRdd, keyName, fnames, expectedTypes, vecIndices.map(elemMaxSizes(_)),
      sparse = sparseInfo, perSQLPartition(hc.getConf, elemMaxSizes, elemStartIndices, vecIndices))
  }

  /**
    * @param conf             H2O conf
    * @param keyName          key of the frame
    * @param expectedTypes    expected types of H2O vectors after the corresponding data are converted from Spark
    * @param elemMaxSizes     array containing max size of each element in the dataframe
    * @param elemStartIndices array containing positions in h2o frame corresponding to spark frame
    * @param uploadPlan       plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param sparse           identifies which columns are sparse
    * @param context          spark task context
    * @param it               iterator over data in the partition
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perSQLPartition(conf: H2OConf, elemMaxSizes: Array[Int], elemStartIndices: Array[Int], vecIndices: Array[Int])
                     (keyName: String, expectedTypes: Array[Byte], uploadPlan: Option[UploadPlan],
                      writeTimeout: Int, driverTimeStamp: Short, sparse: Array[Boolean], partitions: Seq[Int])
                     (context: TaskContext, it: Iterator[Row]): (Int, Long) = {
    val chunkIdx = partitions.indexOf(context.partitionId())
    val (iterator, dataSize) = WriteConverterCtxUtils.bufferedIteratorWithSize(uploadPlan, it)
    val con = WriteConverterCtxUtils.create(conf, uploadPlan, chunkIdx)
    // Collect mapping start position of vector and its size
    val vecStartSize = (for (vecIdx <- vecIndices) yield {
      (elemStartIndices(vecIdx), elemMaxSizes(vecIdx))
    }).toMap
    // Creates array of H2O NewChunks; A place to record all the data in this partition
    con.createChunk(keyName, dataSize, expectedTypes, chunkIdx, vecIndices.map(elemMaxSizes(_)), sparse, vecStartSize)

    var localRowIdx = 0
    iterator.foreach { row =>
      sparkRowToH2ORow(row, localRowIdx, con, elemStartIndices, elemMaxSizes)
      localRowIdx += 1
    }

    //Compress & write data in partitions to H2O Chunks
    con.closeChunks(localRowIdx)

    // Return H2O chunk index and number of rows in this chunk
    (chunkIdx, con.numOfRows())
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
          case BooleanType => con.put(idxH2O, row.getBoolean(idxField))
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
          case _: ml.linalg.VectorUDT => con.putVector(idxH2O, row.getAs[ml.linalg.Vector](idxField), elemSizes(idxField))
          case _: mllib.linalg.VectorUDT => con.putVector(idxH2O, row.getAs[mllib.linalg.Vector](idxField), elemSizes(idxField))
          case udt: UserDefinedType[_] => throw new UnsupportedOperationException(s"User defined type is not supported: ${udt.getClass}")
          case _ => con.putNA(idxH2O)
        }
      }
    }
    con.finishRow()
  }
}
