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

import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, StructField}
import org.apache.spark.sql.{Column, Row}

object H2OMOJOPredictionBinomial extends H2OMOJOPrediction {

  private def supportsCalibratedProbabilities(predictWrapper: EasyPredictModelWrapper): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    predictWrapper.m.calibrateClassProbabilities(Array.fill[Double](2)(0))
  }

  override def getPredictionUDF(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): UserDefinedFunction = {
    if (supportsCalibratedProbabilities(predictWrapper)) {
      if (model.getWithDetailedPredictionCol()) {
        udf[WithCalibrationAndContribution, Row] { r: Row =>
          val pred = predictWrapper.predictBinomial(RowConverter.toH2ORowData(r))
          WithCalibrationAndContribution(
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1),
            pred.contributions
          )
        }
      } else {
        udf[WithCalibration, Row] { r: Row =>
          val pred = predictWrapper.predictBinomial(RowConverter.toH2ORowData(r))
          WithCalibration(
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1)
          )
        }
      }
    } else if (model.getWithDetailedPredictionCol()) {
      udf[WithContribution, Row] { r: Row =>
        val pred = predictWrapper.predictBinomial(RowConverter.toH2ORowData(r))
        WithContribution(
          pred.classProbabilities(0),
          pred.classProbabilities(1),
          pred.contributions
        )
      }
    } else {
      udf[Base, Row] { r: Row =>
        val pred = predictWrapper.predictBinomial(RowConverter.toH2ORowData(r))
        Base(
          pred.classProbabilities(0),
          pred.classProbabilities(1)
        )
      }
    }
  }

  private val binomialSchemaBase = Seq("p0", "p1").map(StructField(_, DoubleType, nullable = false))

  override def getPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    if (supportsCalibratedProbabilities(predictWrapper)) {
      binomialSchemaBase ++ Seq("p0_calibrated", "p1_calibrated").map(StructField(_, DoubleType, nullable = false))
    } else {
      binomialSchemaBase
    }
  }

  override def getDetailedPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    if (supportsCalibratedProbabilities(predictWrapper)) {
      val base = binomialSchemaBase ++ Seq("p0_calibrated", "p1_calibrated").map(StructField(_, DoubleType, nullable = false))
      if (model.getWithDetailedPredictionCol()) {
        base ++ Seq(StructField("contributions", ArrayType(FloatType)))
      } else {
        base
      }
    } else if (model.getWithDetailedPredictionCol()) {
      binomialSchemaBase ++ Seq(StructField("contributions", ArrayType(FloatType)))
    } else {
      binomialSchemaBase
    }
  }

  case class Base(p0: Double, p1: Double)

  case class WithCalibration(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double)

  case class WithContribution(p0: Double, p1: Double, contributions: Array[Float])

  case class WithCalibrationAndContribution(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double, contributions: Array[Float])

  override def extractSimplePredictionColumns(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Column = {
    val p0col = col(s"${model.getDetailedPredictionCol()}.p0").as("p0")
    val p1col = col(s"${model.getDetailedPredictionCol()}.p1").as("p1")

    if (supportsCalibratedProbabilities(predictWrapper)) {
      val p0calibratedCol = col(s"${model.getDetailedPredictionCol()}.p0_calibrated").as("p0_calibrated")
      val p1calibratedCol = col(s"${model.getDetailedPredictionCol()}.p1_calibrated").as("p1_calibrated")
      struct(p0col, p1col, p0calibratedCol, p1calibratedCol)
    } else {
      struct(p0col, p1col)
    }
  }
}
