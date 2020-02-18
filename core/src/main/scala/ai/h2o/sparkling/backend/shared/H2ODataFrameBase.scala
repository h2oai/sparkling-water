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

package ai.h2o.sparkling.backend.shared

import ai.h2o.sparkling.backend.external.ExternalH2OBackend
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.SupportedTypes.{SimpleType, SupportedType, bySparkType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * The abstract class contains common methods for client-based and REST-based DataFrames
 */
private[backend] abstract class H2ODataFrameBase(sc: SparkContext, h2OConf: H2OConf) extends RDD[InternalRow](sc, Nil) with H2OSparkEntity {

  override val isExternalBackend = h2OConf.runsInExternalClusterMode

  protected def types: Array[DataType]

  protected def indexToSupportedType(index: Int): SupportedType

  protected def resolveExpectedTypes(): Option[Array[Byte]] = {
    // there is no need to prepare expected types in internal backend
    if (isExternalBackend) {
      // prepare expected type selected columns in the same order as are selected columns
      val javaClasses = selectedColumnIndices.map(indexToSupportedType(_).javaClass)
      Option(ExternalH2OBackend.prepareExpectedTypes(javaClasses))
    } else {
      None
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // Prepare iterator
    val iterator = new H2OChunkIterator[InternalRow] {

      override val conf: H2OConf = h2OConf
      /** Frame reference */
      override val keyName = frameKeyName
      /** Processed partition index */
      override val partIndex = split.index

      private val columnIndicesWithTypes: Array[(Int, SimpleType[_])] = selectedColumnIndices map (i => (i, bySparkType(types(i))))

      /*a sequence of value providers, per column*/
      private val columnValueProviders: Array[() => Option[Any]] = converterCtx.columnValueProviders(columnIndicesWithTypes)

      def readOptionalData: Seq[Option[Any]] = columnValueProviders map (_ ())

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
