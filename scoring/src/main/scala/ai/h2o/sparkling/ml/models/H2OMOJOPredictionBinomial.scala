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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionBinomial._
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionBinomial {
  self: H2OMOJOModel =>

  private def supportsCalibratedProbabilities(predictWrapper: EasyPredictModelWrapper): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    predictWrapper.m.calibrateClassProbabilities(Array.fill[Double](3)(0))
  }

  def getBinomialPredictionUDF(): UserDefinedFunction = {
    logWarning("Starting from the next major release, the content of 'prediction' column will be generated to " +
      " 'detailed_prediction' instead. The 'prediction' column will contain directly the predicted label.")
    if (supportsCalibratedProbabilities(H2OMOJOCache.getMojoBackend(uid, getMojoData, this))) {
      if (getWithDetailedPredictionCol()) {
        udf[WithCalibrationAndContribution, Row, Double] { (r: Row, offset: Double) =>
          val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
            .predictBinomial(RowConverter.toH2ORowData(r), offset)
          WithCalibrationAndContribution(
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1),
            pred.contributions
          )
        }
      } else {
        udf[WithCalibration, Row, Double] { (r: Row, offset: Double) =>
          val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
            .predictBinomial(RowConverter.toH2ORowData(r), offset)
          WithCalibration(
            pred.classProbabilities(0),
            pred.classProbabilities(1),
            pred.calibratedClassProbabilities(0),
            pred.calibratedClassProbabilities(1)
          )
        }
      }
    } else if (getWithDetailedPredictionCol()) {
      udf[WithContribution, Row, Double] { (r: Row, offset: Double) =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
          .predictBinomial(RowConverter.toH2ORowData(r), offset)
        WithContribution(
          pred.classProbabilities(0),
          pred.classProbabilities(1),
          pred.contributions
        )
      }
    } else {
      udf[Base, Row, Double] { (r: Row, offset: Double) =>
        val pred = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
          .predictBinomial(RowConverter.toH2ORowData(r), offset)
        Base(
          pred.classProbabilities(0),
          pred.classProbabilities(1)
        )
      }
    }
  }

  private val baseFields = Seq("p0", "p1").map(StructField(_, DoubleType, nullable = false))

  def getBinomialPredictionColSchema(): Seq[StructField] = {
    val fields = if (supportsCalibratedProbabilities(H2OMOJOCache.getMojoBackend(uid, getMojoData, this))) {
      baseFields ++ Seq("p0_calibrated", "p1_calibrated").map(StructField(_, DoubleType, nullable = false))
    } else {
      baseFields
    }

    Seq(StructField(getPredictionCol(), StructType(fields), nullable = false))
  }

  def getBinomialDetailedPredictionColSchema(): Seq[StructField] = {
    val fields = if (supportsCalibratedProbabilities(H2OMOJOCache.getMojoBackend(uid, getMojoData, this))) {
      val base = baseFields ++ Seq("p0_calibrated", "p1_calibrated").map(StructField(_, DoubleType, nullable = false))
      if (getWithDetailedPredictionCol()) {
        base ++ Seq(StructField("contributions", ArrayType(FloatType)))
      } else {
        base
      }
    } else if (getWithDetailedPredictionCol()) {
      baseFields ++ Seq(StructField("contributions", ArrayType(FloatType)))
    } else {
      baseFields
    }

    Seq(StructField(getDetailedPredictionCol(), StructType(fields), nullable = false))
  }

  def extractBinomialPredictionColContent(): Column = {
    if (supportsCalibratedProbabilities(H2OMOJOCache.getMojoBackend(uid, getMojoData, this))) {
      extractColumnsAsNested(Seq("p0", "p1", "p0_calibrated", "p1_calibrated"))
    } else {
      extractColumnsAsNested(Seq("p0", "p1"))
    }
  }
}

object H2OMOJOPredictionBinomial {

  case class Base(p0: Double, p1: Double)

  case class WithCalibration(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double)

  case class WithContribution(p0: Double, p1: Double, contributions: Array[Float])

  case class WithCalibrationAndContribution(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double, contributions: Array[Float])

}
