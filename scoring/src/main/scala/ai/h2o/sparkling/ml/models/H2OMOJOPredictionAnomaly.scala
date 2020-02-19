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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionAnomaly.{Base, Detailed}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionAnomaly {
  self: H2OMOJOModel =>
  def getAnomalyPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      udf[Detailed, Row] { r: Row =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this).predictAnomalyDetection(RowConverter.toH2ORowData(r))
        Detailed(pred.score, pred.normalizedScore)
      }
    }
    else {
      udf[Base, Row] { r: Row =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this).predictAnomalyDetection(RowConverter.toH2ORowData(r))
        Base(pred.score)
      }
    }
  }

  private val predictionColType = DoubleType
  private val predictionColNullable = true

  def getAnomalyPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getAnomalyDetailedPredictionColSchema(): Seq[StructField] = {
    val scoreField = StructField("score", predictionColType, nullable = false)
    val fields = if (getWithDetailedPredictionCol()) {
      val normalizedScoreField = StructField("normalizedScore", predictionColType, nullable = false)
      scoreField :: normalizedScoreField :: Nil
    } else {
      scoreField :: Nil
    }
    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))

  }

  def extractAnomalyPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.score")
  }
}

object H2OMOJOPredictionAnomaly {

  case class Base(score: Double)

  case class Detailed(score: Double, normalizedScore: Double)

}
