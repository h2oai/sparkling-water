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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionRegression.{Base, WithAssignments, WithContributions, WithContributionsAndAssignments}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionRegression extends PredictionWithContributions {
  self: H2OMOJOModel =>

  def getRegressionPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      if (getWithContributions() && getWithLeafNodeAssignments()) {
        udf[WithContributionsAndAssignments, Row, Double] { (r: Row, offset: Double) =>
          val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
          val pred = model.predictRegression(RowConverter.toH2ORowData(r), offset)
          val contributions = convertContributionsToMap(model, pred.contributions)
          WithContributionsAndAssignments(pred.value, contributions, pred.leafNodeAssignments)
        }
      } else if (getWithContributions()) {
        udf[WithContributions, Row, Double] { (r: Row, offset: Double) =>
          val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
          val pred = model.predictRegression(RowConverter.toH2ORowData(r), offset)
          val contributions = convertContributionsToMap(model, pred.contributions)
          WithContributions(pred.value, contributions)
        }
      } else if (getWithLeafNodeAssignments()) {
        udf[WithAssignments, Row, Double] { (r: Row, offset: Double) =>
          val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
          val pred = model.predictRegression(RowConverter.toH2ORowData(r), offset)
          WithAssignments(pred.value, pred.leafNodeAssignments)
        }
      } else {
        getBaseUdf()
      }
    } else {
      getBaseUdf()
    }
  }

  private val predictionColType = DoubleType
  private val predictionColNullable = true

  def getRegressionPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getRegressionDetailedPredictionColSchema(): Seq[StructField] = {
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

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))
  }

  def extractRegressionPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.value")
  }

  private def getBaseUdf(): UserDefinedFunction = {
    udf[Base, Row, Double] { (r: Row, offset: Double) =>
      val pred = H2OMOJOCache
        .getMojoBackend(uid, getMojo, this)
        .predictRegression(RowConverter.toH2ORowData(r), offset)
      Base(pred.value)
    }
  }
}

object H2OMOJOPredictionRegression {

  case class Base(value: Double)

  case class WithContributions(value: Double, contributions: Map[String, Float])

  case class WithAssignments(value: Double, leafNodeAssignments: Array[String])

  case class WithContributionsAndAssignments(
      value: Double,
      contributions: Map[String, Float],
      leafNodeAssignments: Array[String])
}
