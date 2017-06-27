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
import org.apache.spark.h2o.backends.external.ExternalBackendUtils
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.h2o.utils.SupportedTypes._
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
    override val readTimeout = hc.getConf.externalReadConfirmationTimeout
  } with RDD[InternalRow](hc.sparkContext, Nil) with H2ORDDLike[T] {

  def this(@transient frame: T)
          (@transient hc: H2OContext) = this(frame, null)(hc)

  val colNames = frame.names()
  val types: Array[DataType] = frame.vecs map ReflectionUtils.dataTypeFor

  // TODO(vlad): take care of the cases when names are missing in colNames - an exception?
  override val selectedColumnIndices = (if (requiredColumns == null) {
    colNames.indices
  } else {
    requiredColumns.toSeq.map(colName => colNames.indexOf(colName))
  }) toArray

  override val expectedTypes: Option[Array[Byte]] = {
    // there is no need to prepare expected types in internal backend
    if (isExternalBackend) {
      // prepare expected type selected columns in the same order as are selected columns
      val javaClasses = selectedColumnIndices.map{ idx => ReflectionUtils.supportedType(frame.vec(idx)).javaClass }
      Option(ExternalBackendUtils.prepareExpectedTypes(javaClasses))
    } else {
      None
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // Prepare iterator
    val iterator = new H2OChunkIterator[InternalRow] {

      /** Frame reference */
      override val keyName = frameKeyName
      /** Processed partition index */
      override val partIndex = split.index

      private val columnIndicesWithTypes: Array[(Int, SimpleType[_])] = selectedColumnIndices map (i => (i, bySparkType(types(i))))

      /*a sequence of value providers, per column*/
      private val columnValueProviders: Array[() => Option[Any]] = converterCtx.columnValueProviders(columnIndicesWithTypes)

      def readOptionalData: Seq[Option[Any]] = columnValueProviders map (_())

      private def readRow: InternalRow = {
          val optionalData: Seq[Option[Any]] = readOptionalData
          val nullableData: Seq[Any] = optionalData map (_ orNull)
          InternalRow.fromSeq(nullableData)
      }

      override def next(): InternalRow = {
        val row = readRow
        converterCtx.increaseRowIdx()
        row
      }
    }

    // Wrap the iterator as a backend specific wrapper
    ReadConverterCtxUtils.backendSpecificIterator[InternalRow](isExternalBackend, iterator)
  }
}
