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

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import water.fvec.Frame

/** Client-based H2O relation implementing column filter operation.
 */
case class InternalBackendH2OFrameRelation[T <: Frame](@transient h2oFrame: T,
                                                       @transient copyMetadata: Boolean)
                                                      (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan /* with PrunedFilterScan */ {

  lazy val h2oContext = H2OContext.ensure("H2OContext has to be started in order to do " +
    "transformations between Spark and H2O frames.")

  // Get rid of annoying print
  override def toString: String = getClass.getSimpleName

  override val needConversion = false

  override val schema: StructType = createSchema(h2oFrame, copyMetadata)

  override def buildScan(): RDD[Row] =
    new InternalBackendH2ODataFrame(h2oFrame)(h2oContext).asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new InternalBackendH2ODataFrame(h2oFrame, requiredColumns)(h2oContext).asInstanceOf[RDD[Row]]

  private def createSchema(f: T, copyMetadata: Boolean): StructType = {
    import ReflectionUtils._

    val types = new Array[StructField](f.numCols())
    val vecs = f.vecs()
    val names = f.names()
    for (i <- 0 until f.numCols()) {
      val vec = vecs(i)
      types(i) = if (copyMetadata) {
        var metadata = new MetadataBuilder()
          .putLong("count", vec.length())
          .putLong("naCnt", vec.naCnt())

        if (vec.isCategorical) {
          metadata
            .putStringArray("vals", vec.domain())
            .putLong("cardinality", vec.cardinality().toLong)
        } else if (vec.isNumeric) {
          metadata
            .putDouble("min", vec.min())
            .putDouble("mean", vec.mean())
            .putDoubleArray("percentiles", vec.pctiles())
            .putDouble("max", vec.max())
            .putDouble("std", vec.sigma())
            .putDouble("sparsity", vec.nzCnt() / vec.length().toDouble)
        }
        StructField(
          names(i), // Name of column
          dataTypeFor(vec), // Catalyst type of column
          vec.naCnt() > 0,
          metadata.build())
      } else {
        StructField(
          names(i), // Name of column
          dataTypeFor(vec), // Catalyst type of column
          vec.naCnt() > 0)
      }
    }
    StructType(types)
  }
}
