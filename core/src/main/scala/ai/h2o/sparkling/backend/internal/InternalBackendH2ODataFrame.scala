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

package ai.h2o.sparkling.backend.internal

import ai.h2o.sparkling.backend.shared.H2ODataFrameBase
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.sql.types.DataType
import water.H2O
import water.support.H2OFrameSupport

import scala.language.postfixOps

/**
 * H2O H2OFrame wrapper providing RDD[Row]=DataFrame API.
 *
 * @param frame           frame which will be wrapped as DataFrame
 * @param requiredColumns list of the columns which should be provided by iterator, null means all
 * @param hc              an instance of H2O Context
 */
private[backend] class InternalBackendH2ODataFrame[T <: water.fvec.Frame](@transient val frame: T,
                                                                          val requiredColumns: Array[String])
                                                                         (@transient val hc: H2OContext)
  extends H2ODataFrameBase(hc.sparkContext, hc.getConf) with InternalBackendSparkEntity[T] {

  def this(@transient frame: T)
          (@transient hc: H2OContext) = this(frame, null)(hc)

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

  override val expectedTypes: Option[Array[VecType]] = resolveExpectedTypes()

  protected override def indexToSupportedType(index: Int): SupportedType = {
    ReflectionUtils.supportedType(frame.vec(index))
  }
}
