package ai.h2o.sparkling.ml.models

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import ai.h2o.sparkling.sql.functions.udf
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionClustering {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = IntegerType
  private val predictionColNullable = true

  def getClusteringPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config])
      : UserDefinedFunction = {
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val pred = model.predictClustering(RowConverter.toH2ORowData(r))
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred.cluster
      resultBuilder += pred.distances

      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getClusteringPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getClusteringPredictionSchema(): StructType = {
    val clusterField = StructField("cluster", predictionColType, nullable = predictionColNullable)
    val distancesField = StructField("distances", ArrayType(DoubleType, containsNull = false), nullable = true)
    val fields = clusterField :: distancesField :: Nil
    StructType(fields)
  }

  def extractClusteringPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.cluster")
  }
}
