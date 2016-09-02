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
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.language.postfixOps

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

  val typesAll: Array[DataType] = frame.vecs map ReflectionUtils.dataTypeFor

  /** Create new types list which describes expected types in a way external H2O backend can use it. This list
    * contains types in a format same for H2ODataFrame and H2ORDD */
  lazy val expectedTypesAll: Option[Array[Byte]] = ConverterUtils.prepareExpectedTypes(isExternalBackend, typesAll)

  val colNames = frame.names()

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // Prepare iterator
    val iterator = new H2OChunkIterator[InternalRow] {

      /** Frame reference */
      override val keyName = frameKeyName
      /** Processed partition index */
      override val partIndex = split.index

      // TODO(vlad): take care of the cases when names are missing in colNames - an exception?
      val selectedColumnIndices = (if (requiredColumns == null) {
        colNames.indices
      } else {
        requiredColumns.toSeq.map{ name => colNames.indexOf(name) }
      }) toArray

    /** Filtered list of types used for data transfer */
      val expectedTypes: Option[Array[Byte]] =
        expectedTypesAll map (selectedColumnIndices map _)

      /* Converter context */
      override val converterCtx: ReadConverterContext =
      ConverterUtils.getReadConverterContext(isExternalBackend,
        keyName,
        chksLocation,
        expectedTypes,
        partIndex)


      private val columnIndicesWithTypes: Array[(Int, DataType)] = selectedColumnIndices map (i => (i, typesAll(i)))

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

    // TODO(vlad): get rid of booleanness
    // Wrap the iterator to backend specific wrapper
    ConverterUtils.getIterator[InternalRow](isExternalBackend, iterator)
  }
}
