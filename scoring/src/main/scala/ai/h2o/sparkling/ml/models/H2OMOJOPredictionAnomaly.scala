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

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionAnomaly {
  self: H2OMOJOModel =>
  def getAnomalyPredictionUDF(): UserDefinedFunction = {
    val schema = getAnomalyPredictionSchema()
    val function = (r: Row) => {
      val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
      val pred = model.predictAnomalyDetection(RowConverter.toH2ORowData(r))
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.score
      resultBuilder += pred.normalizedScore
      if (getWithLeafNodeAssignments()) {
        resultBuilder += pred.leafNodeAssignments
      }
      if (getWithStageResults()) {
        resultBuilder += pred.stageProbabilities
      }
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  private val predictionColType = DoubleType
  private val predictionColNullable = true

  def getAnomalyPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getAnomalyPredictionSchema(): StructType = {
    val scoreField = StructField("score", predictionColType, nullable = false)
    val normalizedScoreField = StructField("normalizedScore", predictionColType, nullable = false)
    val baseFields = scoreField :: normalizedScoreField :: Nil
    val assignmentFields = if (getWithLeafNodeAssignments()) {
      val assignmentsField =
        StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
      baseFields :+ assignmentsField
    } else {
      baseFields
    }
    val stageResultFields = if (getWithStageResults()) {
      val stageResultsField =
        StructField("stageResults", ArrayType(DoubleType, containsNull = false), nullable = false)
      assignmentFields :+ stageResultsField
    } else {
      assignmentFields
    }
    StructType(stageResultFields)
  }

  def extractAnomalyPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.score")
  }
}
