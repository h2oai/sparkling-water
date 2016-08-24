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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.h2o.utils.H2OSchemaUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
 *
 * @param frame frame which will be wrapped as DataFrame
 * @param requiredColumns  list of the columns which should be provided by iterator, null means all
 * @param sc an instance of Spark context
 */
private[spark]
class H2ODataFrame[T <: water.fvec.Frame](@transient val frame: T,
                                          val requiredColumns: Array[String])
                                         (@transient val sc: SparkContext) extends {
  override val isExternalBackend = H2OConf(sc).runsInExternalClusterMode
}
  with RDD[InternalRow](sc, Nil) with H2ORDDLike[T] {

  def this(@transient frame: T)
          (@transient sc: SparkContext) = this(frame, null)(sc)


  val typesAll = frame.vecs().indices.map(idx => vecTypeToDataType(frame.vec(idx))).toArray
  /** Create new types list which describes expected types in a way external H2O backend can use it. This list
    * contains types in a format same for H2ODataFrame and H2ORDD */
  val expectedTypesAll: Option[Array[Byte]] = ConverterUtils.prepareExpectedTypes(isExternalBackend, typesAll)
  val colNames = frame.names()

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // Prepare iterator
    val iterator = new H2OChunkIterator[InternalRow] {
      /** Frame reference */
      override val keyName = frameKeyName
      /** Processed partition index */
      override val partIndex = split.index
      /** Selected column indices */
      val selectedColumnIndices = if (requiredColumns == null) {
        colNames.indices
      } else {
        requiredColumns.toSeq.map{ name => colNames.indexOf(name) }
        }

      // Make sure that column selection is consistent
      // scalastyle:off
      assert(requiredColumns != null && selectedColumnIndices.length == requiredColumns.length,
             "Column selection missing a column!")
      // scalastyle:on

      val filteredTypes = selectedColumnIndices.map(idx => typesAll(idx)).toArray
      /** Filtered list of types used for data transfer */
      val expectedTypes: Option[Array[Byte]]  =
      if (expectedTypesAll.isDefined){
        Some(selectedColumnIndices.map(idx => expectedTypesAll.get(idx)).toArray)
      }else{
        None
      }

      /* Converter context */
      override val converterCtx: ReadConverterContext =
      ConverterUtils.getReadConverterContext(isExternalBackend,
        keyName,
        chksLocation,
        expectedTypes,
        partIndex)

      override def next(): InternalRow = {
        /** Mutable reusable row returned by iterator */
        val mutableRow = new GenericMutableRow(selectedColumnIndices.length)
        selectedColumnIndices.indices.foreach { idx =>
          val i = selectedColumnIndices(idx)
          val typ = filteredTypes(idx)
          if (converterCtx.isNA(i)) {
            mutableRow.setNullAt(idx)
          } else {
            typ match {
              case ByteType => mutableRow.setByte(idx, converterCtx.getByte(i))
              case ShortType => mutableRow.setShort(idx, converterCtx.getShort(i))
              case IntegerType => mutableRow.setInt(idx, converterCtx.getInt(i))
              case LongType => mutableRow.setLong(idx, converterCtx.getLong(i))
              case FloatType => mutableRow.setFloat(idx, converterCtx.getFloat(i))
              case DoubleType => mutableRow.setDouble(idx, converterCtx.getDouble(i))
              case BooleanType => mutableRow.setBoolean(idx, converterCtx.getBoolean(i))
              case StringType => mutableRow.update(idx, converterCtx.getUTF8String(i))
              case TimestampType => mutableRow.setLong(idx, converterCtx.getTimestamp(i))
              case _ => ???
            }
          }
        }
        converterCtx.increaseRowIdx()
        // Return result
        mutableRow
      }
    }

    // TODO(vlad): get rid of booleanness
    // Wrap the iterator to backend specific wrapper
    ConverterUtils.getIterator[InternalRow](isExternalBackend, iterator)
  }
}
