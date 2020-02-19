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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionRegression.{Base, WithContributions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionRegression {
  self: H2OMOJOModel =>

  def getRegressionPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      udf[WithContributions, Row, Double] { (r: Row, offset: Double) =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
          .predictRegression(RowConverter.toH2ORowData(r), offset)
        WithContributions(pred.value, pred.contributions)
      }
    } else {
      udf[Base, Row, Double] { (r: Row, offset: Double) =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
          .predictRegression(RowConverter.toH2ORowData(r), offset)
        Base(pred.value)
      }
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
      val contributionsField = StructField("contributions", ArrayType(FloatType, containsNull = false), nullable = true)
      valueField :: contributionsField :: Nil
    } else {
      valueField :: Nil
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = true))
  }

  def extractRegressionPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.value")
  }
}

object H2OMOJOPredictionRegression {

  case class Base(value: Double)

  case class WithContributions(value: Double, contributions: Array[Float])

}
