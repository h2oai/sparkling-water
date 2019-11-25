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

import ai.h2o.sparkling.frame.H2OFrame
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.backends.external.ExternalH2OBackend
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.H2O
import water.support.H2OFrameSupport

import scala.language.postfixOps

/**
  * The abstract class contains common methods for client-based and REST-based DataFrames
  */
private[spark]
abstract class H2ODataFrameBase(sc: SparkContext) extends RDD[InternalRow](sc, Nil) with H2OSparkEntity {

  protected def types: Array[DataType]

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
  extends H2ODataFrameBase(hc.sparkContext) with H2OClientBasedSparkEntity[T] {

  def this(@transient frame: T)
          (@transient hc: H2OContext) = this(frame, null)(hc)

  override val isExternalBackend = hc.getConf.runsInExternalClusterMode
  override val driverTimeStamp = H2O.SELF.getTimestamp()

  H2OFrameSupport.lockAndUpdate(frame)
  private val colNames = frame.names()
  protected override val types: Array[DataType] = frame.vecs map ReflectionUtils.dataTypeFor

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
      Option(ExternalH2OBackend.prepareExpectedTypes(javaClasses))
    } else {
      None
    }
  }
}

/**
  * H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
  *
  * @param frame frame which will be wrapped as DataFrame
  * @param requiredColumns  list of the columns which should be provided by iterator, null means all
  * @param hc an instance of H2O Context
  */
private[spark]
class H2ORESTDataFrame(@transient val frame: H2OFrame, val requiredColumns: Array[String])
                      (@transient val hc: H2OContext)
  extends H2ODataFrameBase(hc.sparkContext) with H2ORESTBasedSparkEntity {

  def this(@transient frame: H2OFrame)
          (@transient hc: H2OContext) = this(frame, null)(hc)

  override val isExternalBackend = hc.getConf.runsInExternalClusterMode

  private val colNames = frame.columns.map(_.name)

  protected override val types: Array[DataType] = frame.columns.map(c => ReflectionUtils.dataTypeFor(c.dataType))

  override val selectedColumnIndices: Array[Int] = {
    if (requiredColumns == null) {
      colNames.indices.toArray
    } else {
      requiredColumns.map(colNames.indexOf)
    }
  }

  override lazy val expectedTypes: Option[Array[Byte]] = {
    // there is no need to prepare expected types in internal backend
    if (isExternalBackend) {
      // prepare expected type selected columns in the same order as are selected columns
      val javaClasses = selectedColumnIndices.map { idx =>
        val columnType = frame.columns(idx).dataType
        ReflectionUtils.supportedType(columnType).javaClass
      }
      Option(ExternalH2OBackend.prepareExpectedTypes(javaClasses))
    } else {
      None
    }
  }
}
