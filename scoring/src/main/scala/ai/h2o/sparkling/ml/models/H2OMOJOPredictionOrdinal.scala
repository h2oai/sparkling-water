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

trait H2OMOJOPredictionOrdinal {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = StringType
  private val predictionColNullable = true

  def getOrdinalPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row, offset: Double) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val pred = model.predictOrdinal(RowConverter.toH2ORowData(r), offset)
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.label
      resultBuilder += Utils.arrayToRow(pred.classProbabilities)
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getOrdinalPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getOrdinalPredictionSchema(): StructType = {
    val labelField = StructField("label", predictionColType, nullable = predictionColNullable)

    val model = loadEasyPredictModelWrapper()
    val classFields = model.getResponseDomainValues.map(StructField(_, DoubleType, nullable = false))
    val probabilitiesField = StructField("probabilities", StructType(classFields), nullable = false)
    val fields = labelField :: probabilitiesField :: Nil

    StructType(fields)
  }

  def extractOrdinalPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.label")
  }
}
