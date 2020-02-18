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

package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.backend.shared.{H2ODataFrameBase, Reader}
import ai.h2o.sparkling.frame.H2OFrame
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.h2o.utils.SupportedTypes.{SupportedType, VecType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, TaskContext}

/**
 * H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
 *
 * @param frame           frame which will be wrapped as DataFrame
 * @param requiredColumns list of the columns which should be provided by iterator, null means all
 * @param hc              an instance of H2O Context
 */
private[backend] class ExternalBackendH2ODataFrame(val frame: H2OFrame, val requiredColumns: Array[String])
                                                  (@transient val hc: H2OContext)
  extends RDD[InternalRow](hc.sparkContext, Nil) with H2ODataFrameBase with ExternalBackendSparkEntity {

  private val h2oConf = hc.getConf

  def this(frame: H2OFrame)(@transient hc: H2OContext) = this(frame, null)(hc)

  private val colNames = frame.columns.map(_.name)

  protected override val types: Array[DataType] = frame.columns.map(ReflectionUtils.dataTypeFor)

  override val selectedColumnIndices: Array[Int] = {
    if (requiredColumns == null) {
      colNames.indices.toArray
    } else {
      requiredColumns.map(colNames.indexOf)
    }
  }

  override val expectedTypes: Option[Array[VecType]] = {
    // prepare expected type selected columns in the same order as are selected columns
    val javaClasses = selectedColumnIndices.map(indexToSupportedType(_).javaClass)
    Option(ExternalH2OBackend.prepareExpectedTypes(javaClasses))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    // When user ask to read whatever number of rows, buffer them all, because we can't keep the connection
    // to h2o opened indefinitely(spark works in a lazy way)
    new H2ODataFrameIterator {
      private val chnk = frame.chunks.find(_.index == split.index).head
      override val reader: Reader = new ExternalBackendReader(frameKeyName, split.index, chnk.numberOfRows,
        chnk.location, expectedTypes.get, selectedColumnIndices, h2oConf)
    }.toList.toIterator
  }

  protected override def indexToSupportedType(index: Int): SupportedType = {
    val column = frame.columns(index)
    ReflectionUtils.supportedType(column)
  }
}
