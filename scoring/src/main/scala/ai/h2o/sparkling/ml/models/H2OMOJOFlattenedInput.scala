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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.{DatasetShape, SchemaUtils}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{DataFrame, Dataset}

trait H2OMOJOFlattenedInput {
  protected def inputColumnNames: Array[String]

  protected def outputColumnName: String

  protected def applyPredictionUdf(
      dataset: Dataset[_],
      udfConstructor: Array[String] => UserDefinedFunction): DataFrame = {
    val originalDF = dataset.toDF()
    DatasetShape.getDatasetShape(dataset.schema) match {
      case DatasetShape.Flat => applyPredictionUdfToFlatDataFrame(originalDF, udfConstructor, inputColumnNames)
      case DatasetShape.StructsOnly | DatasetShape.Nested =>
        val flattenedDF = SchemaUtils.appendFlattenedStructsToDataFrame(originalDF, RowConverter.temporaryColumnPrefix)
        val inputs = inputColumnNames ++ inputColumnNames.map(s => RowConverter.temporaryColumnPrefix + "." + s)
        val flatWithPredictionsDF = applyPredictionUdfToFlatDataFrame(flattenedDF, udfConstructor, inputs)
        flatWithPredictionsDF.schema.foldLeft(flatWithPredictionsDF) { (df, field) =>
          if (field.name.startsWith(RowConverter.temporaryColumnPrefix)) df.drop(field.name) else df
        }
    }
  }

  protected def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = flatDataFrame.columns.intersect(inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)
    flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*)))
  }
}
