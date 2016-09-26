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

package org.apache.spark.sql

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.converters.H2ODataFrame
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType
import water.fvec.Frame

/**
 * A bridge to access SQLContext private methods.
 */
object H2OSQLContextUtils {

  private[spark] def internalCreateDataFrame(catalystRows: RDD[InternalRow], schema: StructType)
                                            (sqlContext: SQLContext) =
    sqlContext.internalCreateDataFrame(catalystRows, schema)
}

/** H2O relation implementing column filter operation.
  */
case class H2OFrameRelation[T <: Frame](@transient h2oFrame: T,
                                        @transient copyMetadata: Boolean)
                                       (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan /* with PrunedFilterScan */  {

  lazy val h2oContext = H2OContext.get().getOrElse(throw new RuntimeException("H2OContext has to be started in order to do " +
    "transformations between spark and h2o frames"))
  // Get rid of annoying print
  override def toString: String = getClass.getSimpleName

  override val needConversion = false

  override val schema: StructType = H2OSchemaUtils.createSchema(h2oFrame, copyMetadata)

  override def buildScan(): RDD[Row] =
    new H2ODataFrame(h2oFrame)(h2oContext).asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new H2ODataFrame(h2oFrame, requiredColumns)(h2oContext).asInstanceOf[RDD[Row]]
}
