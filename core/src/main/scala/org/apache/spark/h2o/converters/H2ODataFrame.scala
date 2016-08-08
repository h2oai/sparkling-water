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
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.H2OSchemaUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, TaskContext}

import scala.language.postfixOps

/**
 * H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
 *
 * @param frame frame which will be wrapped as DataFrame
 * @param requiredColumns  list of the columns which should be provided by iterator, null means all
 * @param hc an instance of H2O Context
 */
private[spark]
class H2ODataFrame[T <: water.fvec.Frame](@transient val frame: T,
                                          val requiredColumns: Array[String])
                                         (@transient val hc: H2OContext)
  extends {
    override val isExternalBackend = hc.getConf.runsInExternalClusterMode
  } with RDD[InternalRow](hc.sparkContext, Nil) with H2ORDDLike[T] {

  def this(@transient frame: T)
          (@transient hc: H2OContext) = this(frame, null)(hc)


  val types: Array[DataType] = frame.vecs().indices.map(idx => vecTypeToDataType(frame.vec(idx))).toArray
  override val expectedTypes: Option[Array[Byte]] = ConverterUtils.prepareExpectedTypes(isExternalBackend, types)
  val colNames = frame.names()

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // Prepare iterator
    val iterator = new H2OChunkIterator[InternalRow] {

      override val keyName = frameKeyName
      override val partIndex = split.index
      // TODO(vlad): take care of the cases when names are missing in colNames - an exception?
      override val selectedColumnIndices = (if (requiredColumns == null) {
        colNames.indices
      } else {
        requiredColumns.toSeq.map{ name => colNames.indexOf(name) }
      }) toArray

      // Make sure that column selection is consistent
      // scalastyle:off
      assert(requiredColumns != null && selectedColumnIndices.length == requiredColumns.length,
        "Column selection missing a column!")
      // scalastyle:on

      private val filteredTypes = selectedColumnIndices map types

      /*a sequence of converters, per column*/
      private val columnConverters = filteredTypes map converterCtx.get

      private def readRow: InternalRow = {
          val optionalData: Seq[Option[Any]] =
            columnConverters.zipWithIndex map {
              case (converter, idx) => converter(selectedColumnIndices(idx))
            }

          val nullableData: Seq[Any] = optionalData map (_ orNull)

          InternalRow.fromSeq(nullableData)
      }

      override def next(): InternalRow = {
        val row = readRow
        converterCtx.increaseRowIdx()
        row
      }
    }

    // TODO(vlad): get rid of booleanness
    // Wrap the iterator to backend specific wrapper
    ConverterUtils.getIterator[InternalRow](isExternalBackend, iterator)
  }

}