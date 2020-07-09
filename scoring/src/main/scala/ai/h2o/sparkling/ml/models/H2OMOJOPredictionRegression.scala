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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionRegression extends PredictionWithContributions {
  self: H2OMOJOModel =>

  def getRegressionPredictionUDF(): UserDefinedFunction = {
    val schema = getRegressionPredictionSchema()
    val function = (r: Row, offset: Double) => {
      val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
      val pred = model.predictRegression(RowConverter.toH2ORowData(r), offset)
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.value
      if (getWithDetailedPredictionCol()) {
        if (getWithContributions()) {
          resultBuilder += convertContributionsToMap(model, pred.contributions)
        }
        if (getWithLeafNodeAssignments()) {
          resultBuilder += pred.leafNodeAssignments
        }
      }
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  private val predictionColType = DoubleType
  private val predictionColNullable = true

  def getRegressionPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getRegressionPredictionSchema(): StructType = {
    val valueField = StructField("value", DoubleType, nullable = false)
    val fields = if (getWithDetailedPredictionCol()) {
      if (getWithContributions() && getWithLeafNodeAssignments()) {
        val contributionsField = StructField("contributions", getContributionsSchema(), nullable = true)
        val assignmentsField =
          StructField("leafNodeAssignments", ArrayType(StringType, containsNull = true), nullable = true)
        valueField :: contributionsField :: assignmentsField :: Nil
      } else if (getWithContributions()) {
        val contributionsField = StructField("contributions", getContributionsSchema(), nullable = true)
        valueField :: contributionsField :: Nil
      } else if (getWithLeafNodeAssignments()) {
        val assignmentsField =
          StructField("leafNodeAssignments", ArrayType(StringType, containsNull = true), nullable = true)
        valueField :: assignmentsField :: Nil
      } else {
        valueField :: Nil
      }
    } else {
      valueField :: Nil
    }

    StructType(fields)
  }

  def extractRegressionPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.value")
  }
}
