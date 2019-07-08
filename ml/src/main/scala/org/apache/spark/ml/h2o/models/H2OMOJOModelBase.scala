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

package org.apache.spark.ml.h2o.models

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.h2o.param.H2OMOJOModelParams
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

abstract class H2OMOJOModelBase[T <: SparkModel[T]]
  extends SparkModel[T] with H2OMOJOModelParams with MLWritable with HasMojoData {

  protected def getPredictionSchema(): Seq[StructField]

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // Here we should check validity of input schema however
    // in theory user can pass invalid schema with missing columns
    // and model will be able to still provide a prediction
    StructType(schema.fields ++ getPredictionSchema())
  }

  override def write: MLWriter = new H2OMOJOWriter(this, getMojoData)

  protected def applyPredictionUdf(
      dataset: Dataset[_],
      udfConstructor: Array[String] => UserDefinedFunction): DataFrame = {
    val originalDF = dataset.toDF()
    if (H2OSchemaUtils.isSchemaFlat(originalDF.schema)) {
      applyPredictionUdfToFlatDataFrame(originalDF, udfConstructor)
    } else {
      val temporaryIdColumnName = "SparklingWater_MOJO_temporary_id_for_join"
      val withIdentifierDF = originalDF.withColumn(temporaryIdColumnName, monotonically_increasing_id()).cache()
      val flattenedDF = H2OSchemaUtils.flattenDataFrame(withIdentifierDF)
      val flatWithPredictionsDF = applyPredictionUdfToFlatDataFrame(flattenedDF, udfConstructor)
      val predictionsOnlyDF = flatWithPredictionsDF.select(temporaryIdColumnName, getPredictionCol())
      val joinedDF = withIdentifierDF.join(predictionsOnlyDF, temporaryIdColumnName :: Nil, joinType = "left")
      joinedDF.drop(temporaryIdColumnName)
    }
  }

  private def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction): DataFrame = {
    val relevantColumnNames = flatDataFrame.columns.intersect(getFeaturesCols())
    val args = relevantColumnNames.map(flatDataFrame(_))
    val udf = udfConstructor(relevantColumnNames)
    flatDataFrame.withColumn(getPredictionCol(), udf(struct(args: _*)))
  }
}
