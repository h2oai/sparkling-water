package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.Utils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionMultinomial extends PredictionWithStageProbabilities {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = StringType
  private val predictionColNullable = true

  def getMultinomialPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row, offset: Double) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val pred = model.predictMultinomial(RowConverter.toH2ORowData(r), offset)
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.label
      resultBuilder += Utils.arrayToRow(pred.classProbabilities)
      if (model.getEnableLeafAssignment()) {
        resultBuilder += pred.leafNodeAssignments
      }
      if (model.getEnableStagedProbabilities()) {
        val stageProbabilities = pred.stageProbabilities
        val stageProbabilitiesByTree = stageProbabilities.grouped(model.getResponseDomainValues.size).toArray
        val stageProbabilitiesByClass = stageProbabilitiesByTree.transpose
        Utils.arrayToRow(stageProbabilitiesByClass)
        resultBuilder += Utils.arrayToRow(stageProbabilitiesByClass)
      }
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getMultinomialPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getMultinomialPredictionSchema(): StructType = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)

    val model = loadEasyPredictModelWrapper()
    val classFields = model.getResponseDomainValues.map(StructField(_, DoubleType, nullable = false))
    val probabilitiesField =
      StructField("probabilities", StructType(classFields), nullable = false)
    val baseFields = labelField :: probabilitiesField :: Nil
    val assignmentFields = if (model.getEnableLeafAssignment()) {
      val assignmentsField =
        StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
      baseFields :+ assignmentsField
    } else {
      baseFields
    }
    val fields = if (model.getEnableStagedProbabilities()) {
      val stageProbabilitiesField =
        StructField("stageProbabilities", getStageProbabilitiesSchema(model), nullable = false)
      assignmentFields :+ stageProbabilitiesField
    } else {
      assignmentFields
    }

    StructType(fields)
  }

  def extractMultinomialPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}
