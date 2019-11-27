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
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionRegression {
  self: H2OMOJOModel =>

  def getRegressionPredictionUDF(): UserDefinedFunction = {
    logWarning("Starting from the next major release, the content of 'prediction' column will be generated to " +
      " 'detailed_prediction' instead. The 'prediction' column will contain directly the predicted value.")
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

  private val baseField = StructField("value", DoubleType, nullable = false)

  def getRegressionPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), StructType(baseField :: Nil), nullable = false))
  }

  def getRegressionDetailedPredictionColSchema(): Seq[StructField] = {
    val fields = if (getWithDetailedPredictionCol()) {
      val contributionsField = StructField("contributions", ArrayType(FloatType))
      baseField :: contributionsField :: Nil
    } else {
      baseField :: Nil
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = false))
  }

  def extractRegressionPredictionColContent(): Column = {
    extractColumnsAsNested(Seq("value"))
  }
}

object H2OMOJOPredictionRegression {

  case class Base(value: Double)

  case class WithContributions(value: Double, contributions: Array[Float])

}
