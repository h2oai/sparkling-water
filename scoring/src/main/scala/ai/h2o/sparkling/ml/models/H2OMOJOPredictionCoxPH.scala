package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.Utils
import ai.h2o.sparkling.sql.functions.udf
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionCoxPH {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = DoubleType
  private val predictionColNullable = true

  def getCoxPHPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val pred = model.predictCoxPH(RowConverter.toH2ORowData(r))
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.value
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getCoxPHPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getCoxPHPredictionSchema(): StructType = {
    val valueField = StructField("value", DoubleType, nullable = false)

    StructType(valueField :: Nil)
  }

  def extractCoxPHPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.value")
  }
}
