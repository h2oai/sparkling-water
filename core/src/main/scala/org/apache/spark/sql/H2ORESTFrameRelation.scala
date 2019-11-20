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
import org.apache.spark.h2o.converters.H2ORESTDataFrame
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import water.api.schemas3.FrameV3

/** REST-based H2O relation implementing column filter operation.
  */
case class H2ORESTFrameRelation(@transient h2oFrame: FrameV3, @transient copyMetadata: Boolean)
                                       (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan /* with PrunedFilterScan */ {

  lazy val h2oContext = H2OContext.get().getOrElse(throw new RuntimeException("H2OContext has to be started in order to do " +
    "transformations between spark and h2o frames"))

  // Get rid of annoying print
  override def toString: String = getClass.getSimpleName

  override val needConversion = false

  override val schema: StructType = createSchema(h2oFrame, copyMetadata)

  override def buildScan(): RDD[Row] =
    new H2ORESTDataFrame(h2oFrame)(h2oContext).asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new H2ORESTDataFrame(h2oFrame, requiredColumns)(h2oContext).asInstanceOf[RDD[Row]]

  private def createSchema(f: FrameV3, copyMetadata: Boolean): StructType = {
    import ReflectionUtils._

    val types = new Array[StructField](f.column_count)
    val columns = f.columns
    val names = f.columns.map(_.label)
    for (i <- 0 until f.column_count) {
      val column = columns(i)
      types(i) = if (copyMetadata) {
        val metadata = new MetadataBuilder()
          .putLong("count", f.rows)
          .putLong("naCnt", column.missing_count)

        if (column.`type` == "enum") {
          metadata
            .putStringArray("vals", column.domain)
            .putLong("cardinality", column.domain_cardinality)
        } else if (Seq("int", "real").contains(column.`type`)) {
          metadata
            .putDouble("min", column.mins(0))
            .putDouble("mean", column.mean)
            .putDoubleArray("percentiles", column.percentiles)
            .putDouble("max", column.maxs(0))
            .putDouble("std", column.sigma)
            .putDouble("sparsity", column.zero_count / f.rows.toDouble)
        }
        StructField(
          names(i), // Name of column
          dataTypeFor(column.`type`), // Catalyst type of column
          column.missing_count > 0,
          metadata.build())
      } else {
        StructField(
          names(i), // Name of column
          dataTypeFor(column.`type`), // Catalyst type of column
          column.missing_count > 0)
      }
    }
    StructType(types)
  }
}

