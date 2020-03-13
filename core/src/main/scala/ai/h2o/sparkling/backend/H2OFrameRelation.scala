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

package ai.h2o.sparkling.backend

import ai.h2o.sparkling.frame.{H2OColumn, H2OColumnType, H2OFrame}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
 * H2O relation implementing column filter operation.
 */
case class H2OFrameRelation(frame: H2OFrame, copyMetadata: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan {

  private lazy val hc = H2OContext.ensure("H2OContext has to be started in order to do " +
    "transformations between Spark and H2O frames.")

  // Get rid of annoying print
  override def toString: String = getClass.getSimpleName

  override val needConversion = false

  override val schema: StructType = createSchema(frame, copyMetadata)

  override def buildScan(): RDD[Row] =
    new H2ODataFrame(frame)(hc).asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new H2ODataFrame(frame, requiredColumns)(hc).asInstanceOf[RDD[Row]]


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
