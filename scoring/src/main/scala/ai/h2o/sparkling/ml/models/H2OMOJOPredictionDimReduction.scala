package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.Utils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionDimReduction {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = ArrayType(DoubleType, containsNull = false)
  private val predictionColNullable = true

  def getDimReductionPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val pred = model.predictDimReduction(RowConverter.toH2ORowData(r))
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.dimensions
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getDimReductionPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getDimReductionPredictionSchema(): StructType = {
    val fields = StructField("dimensions", predictionColType, nullable = predictionColNullable) :: Nil
    StructType(fields)
  }

  def extractDimReductionSimplePredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.dimensions")
  }
}
