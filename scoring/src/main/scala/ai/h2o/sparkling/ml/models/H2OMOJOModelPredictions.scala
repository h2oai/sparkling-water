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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, StructField}

trait H2OMOJOModelPredictions {
  self: H2OMOJOModel =>

  protected def supportsCalibratedProbabilities(): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    easyPredictModelWrapper.m.calibrateClassProbabilities(Array.fill[Double](2)(0))
  }

  def getBinomialPredictionUDF(): UserDefinedFunction = {
    if (supportsCalibratedProbabilities()) {
      if (getWithDetailedPredictionCol()) {
        udf[BinomialPrediction3, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
          BinomialPrediction3(
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1),
            pred.contributions
          )
        }
      } else {
        udf[BinomialPrediction1, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
          BinomialPrediction1(
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1)
          )
        }
      }
    } else if (getWithDetailedPredictionCol()) {
      udf[BinomialPrediction2, Row] { r: Row =>
        val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
        BinomialPrediction2(
          pred.classProbabilities(0),
          pred.classProbabilities(1),
          pred.contributions
        )
      }
    } else {
      udf[BinomialPrediction0, Row] { r: Row =>
        val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
        BinomialPrediction0(
          pred.classProbabilities(0),
          pred.classProbabilities(1)
        )
      }
    }
  }

  def getBinomialPredictionSchema(): Seq[StructField] = {
    val binomialSchemaBase = Seq("p0", "p1").map(StructField(_, DoubleType, nullable = false))

    if (supportsCalibratedProbabilities()) {
      val base = binomialSchemaBase ++ Seq("p0_calibrated", "p1_calibrated").map(StructField(_, DoubleType, nullable = false))
      if (getWithDetailedPredictionCol()) {
        base ++ Seq(StructField("contributions", ArrayType(FloatType)))
      } else {
        base
      }
    } else if (getWithDetailedPredictionCol())) {
      binomialSchemaBase ++ Seq(StructField("contributions", ArrayType(FloatType)))
    } else {
      binomialSchemaBase
    }
  }

  def getRegressionPredictionUDF(): UserDefinedFunction = {
    if (getWithDetailedPredictionCol()) {
      udf[RegressionPrediction1, Row] { r: Row =>
        val pred = easyPredictModelWrapper.predictRegression(RowConverter.toH2ORowData(r))
        RegressionPrediction1(pred.value, pred.contributions)
      }
    } else {
      udf[RegressionPrediction0, Row] { r: Row =>
        val pred = easyPredictModelWrapper.predictRegression(RowConverter.toH2ORowData(r))
        RegressionPrediction0(pred.value)
      }
    }
  }

  def getRegressionPredictionSchema(): Seq[StructField] = {
    if (getWithDetailedPredictionCol()) {
      StructField("value", DoubleType) :: StructField("contributions", ArrayType(FloatType)) :: Nil
    } else {
      StructField("value", DoubleType) :: Nil
    }
  }
}


case class RegressionPrediction0(value: Double)

case class RegressionPrediction1(value: Double, contributions: Array[Float])

case class BinomialPrediction0(p0: Double, p1: Double)

case class BinomialPrediction1(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double)

case class BinomialPrediction2(p0: Double, p1: Double, contributions: Array[Float])

case class BinomialPrediction3(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double, contributions: Array[Float])
