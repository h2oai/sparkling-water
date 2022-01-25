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

import ai.h2o.sparkling.ml.utils.Utils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionRegression extends PredictionWithContributions {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = DoubleType
  private val predictionColNullable = true

  def getRegressionPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row, offset: Double) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val pred = model.predictRegression(RowConverter.toH2ORowData(r), offset)
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.value
      if (model.getEnableContributions()) {
        resultBuilder += Utils.arrayToRow(pred.contributions)
      }
      if (model.getEnableLeafAssignment()) {
        resultBuilder += pred.leafNodeAssignments
      }
      if (model.getEnableStagedProbabilities()) {
        resultBuilder += pred.stageProbabilities
      }
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getRegressionPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getRegressionPredictionSchema(): StructType = {
    val valueField = StructField("value", DoubleType, nullable = false)
    val baseSchema = valueField :: Nil
    val model = loadEasyPredictModelWrapper()
    val withContributionsSchema = if (model.getEnableContributions()) {
      val contributionsField = StructField("contributions", getContributionsSchema(model), nullable = false)
      baseSchema :+ contributionsField
    } else {
      baseSchema
    }
    val withAssignmentsSchema = if (model.getEnableLeafAssignment()) {
      val assignmentsSchema = ArrayType(StringType, containsNull = false)
      val assignmentsField = StructField("leafNodeAssignments", assignmentsSchema, nullable = false)
      withContributionsSchema :+ assignmentsField
    } else {
      withContributionsSchema
    }
    val fields = if (model.getEnableStagedProbabilities()) {
      val stageResultsField =
        StructField("stageResults", ArrayType(DoubleType, containsNull = false), nullable = false)
      withAssignmentsSchema :+ stageResultsField
    } else {
      withAssignmentsSchema
    }

    StructType(fields)
  }

  def extractRegressionPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.value")
  }
}
