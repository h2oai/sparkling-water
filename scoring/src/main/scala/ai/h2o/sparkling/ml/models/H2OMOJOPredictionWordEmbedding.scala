package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.sql.functions.udf
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row}

import scala.collection.mutable

trait H2OMOJOPredictionWordEmbedding {
  self: H2OAlgorithmMOJOModel =>

  private val predictionColType = ArrayType(FloatType, containsNull = false)
  private val predictionColNullable = true

  def getWordEmbeddingPredictionUDF(
      schema: StructType,
      modelUID: String,
      mojoFileName: String,
      configInitializers: Seq[(EasyPredictModelWrapper.Config) => EasyPredictModelWrapper.Config],
      featureCols: Seq[String]): UserDefinedFunction = {
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(modelUID, mojoFileName, configInitializers)
      val colIdx = model.m.getColIdx(featureCols.head)
      val pred = if (r.isNullAt(colIdx)) {
        null
      } else {
        model.predictWord2Vec(r.getSeq[String](colIdx).toArray)
      }
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

  def getWordEmbeddingPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), predictionColType, nullable = predictionColNullable))
  }

  def getWordEmbeddingPredictionSchema(): StructType = {
    val fields = StructField("wordEmbeddings", predictionColType, nullable = predictionColNullable) :: Nil
    StructType(fields)
  }

  def extractWordEmbeddingPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.wordEmbeddings")
  }
}
