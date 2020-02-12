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

import ai.h2o.sparkling.frame.{H2OColumn, H2OColumnType, H2OFrame}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.converters.H2ORESTDataFrame
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}

/** REST-based H2O relation implementing column filter operation.
  */
case class H2ORESTFrameRelation(frame: H2OFrame, copyMetadata: Boolean)
                               (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan /* with PrunedFilterScan */ {

  lazy val h2oContext = H2OContext.get().getOrElse(throw new RuntimeException("H2OContext has to be started in order to do " +
    "transformations between spark and h2o frames"))

  // Get rid of annoying print
  override def toString: String = getClass.getSimpleName

  override val needConversion = false

  override val schema: StructType = createSchema(frame, copyMetadata)

  override def buildScan(): RDD[Row] =
    new H2ORESTDataFrame(frame)(h2oContext).asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new H2ORESTDataFrame(frame, requiredColumns)(h2oContext).asInstanceOf[RDD[Row]]


  private def extractMetadata(column: H2OColumn, numberOfRows: Long): Metadata = {
    val builder = new MetadataBuilder()
      .putLong("count", numberOfRows)
      .putLong("naCnt", column.numberOfMissingElements)

    if (column.dataType == H2OColumnType.`enum`) {
      builder
        .putLong("cardinality", column.domainCardinality)
      if (column.domain != null) {
        builder.putStringArray("vals", column.domain)
      }
    } else if (Seq(H2OColumnType.int, H2OColumnType.real).contains(column.dataType)) {
      builder
        .putDouble("min", column.min)
        .putDouble("mean", column.mean)
        .putDouble("max", column.max)
        .putDouble("std", column.sigma)
        .putDouble("sparsity", column.numberOfZeros / numberOfRows.toDouble)
      if (column.percentiles != null) {
        builder.putDoubleArray("percentiles", column.percentiles)
      }
    }
    builder.build()
  }

  private def createSchema(f: H2OFrame, copyMetadata: Boolean): StructType = {
    import ReflectionUtils._

    val types = f.columns.map { column =>
      val metadata = if (copyMetadata) extractMetadata(column, f.numberOfRows) else Metadata.empty
      StructField(
        column.name,
        dataTypeFor(column),
        column.nullable,
        metadata)
    }
    StructType(types)
  }
}

