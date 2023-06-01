package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.Utils
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionBinomial extends PredictionWithContributions with PredictionWithStageProbabilities {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = StringType
  private val predictionColNullable = true

  def getBinomialPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    BinomialPredictionUDFClosure.getBinomialPredictionUDF(schema, modelUID, mojoFileName, configInitializers)
  }

  def getBinomialPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getBinomialPredictionSchema(): StructType = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)
    val baseFields = labelField :: Nil

    val model = loadEasyPredictModelWrapper()
    val classFields = model.getResponseDomainValues.map(StructField(_, DoubleType, nullable = false))
    val probabilitiesField = StructField("probabilities", StructType(classFields), nullable = false)
    val detailedPredictionFields = baseFields :+ probabilitiesField

    val contributionsFields = if (model.getEnableContributions()) {
      val contributionsField = StructField("contributions", getContributionsSchema(model), nullable = false)
      detailedPredictionFields :+ contributionsField
    } else {
      detailedPredictionFields
    }

    val assignmentFields = if (model.getEnableLeafAssignment()) {
      val assignmentField =
        StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
      contributionsFields :+ assignmentField
    } else {
      contributionsFields
    }

    val stageProbabilityFields = if (model.getEnableStagedProbabilities()) {
      val stageProbabilitiesField =
        StructField("stageProbabilities", getStageProbabilitiesSchema(model), nullable = false)
      assignmentFields :+ stageProbabilitiesField
    } else {
      assignmentFields
    }

    val fields = if (BinomialPredictionUDFClosure.supportsCalibratedProbabilities(model)) {
      val calibratedProbabilitiesField =
        StructField("calibratedProbabilities", StructType(classFields), nullable = false)
      stageProbabilityFields :+ calibratedProbabilitiesField
    } else {
      stageProbabilityFields
    }

    StructType(fields)
  }

  def extractBinomialPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}

object BinomialPredictionUDFClosure {
  def getBinomialPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row, offset: Double) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val resultBuilder = mutable.ArrayBuffer[Any]()
      val pred = model.predictBinomial(RowConverter.toH2ORowData(r), offset)
      resultBuilder += pred.label
      resultBuilder += Utils.arrayToRow(pred.classProbabilities)
      if (model.getEnableContributions()) {
        resultBuilder += Utils.arrayToRow(pred.contributions)
      }
      if (supportsCalibratedProbabilities(model)) {
        resultBuilder += Utils.arrayToRow(pred.calibratedClassProbabilities)
      }
      if (model.getEnableLeafAssignment()) {
        resultBuilder += pred.leafNodeAssignments
      }
      if (model.getEnableStagedProbabilities()) {
        val p0Array = pred.stageProbabilities
        val p1Array = p0Array.map(1 - _)
        resultBuilder += new GenericRow(Array[Any](p0Array, p1Array))
      }
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def supportsCalibratedProbabilities(predictWrapper: EasyPredictModelWrapper): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    predictWrapper.m.calibrateClassProbabilities(Array.fill[Double](3)(0))
  }
}
