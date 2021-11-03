package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.models.H2OWord2VecMOJOModel
import ai.h2o.sparkling.ml.params.H2OWord2VecExtraParams
import hex.Model
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, explode, size}

import scala.reflect.ClassTag

abstract class H2OWord2VecBase[P <: Model.Parameters: ClassTag]
  extends H2OFeatureEstimator[P]
  with H2OWord2VecExtraParams {

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame)
  }

  override def fit(dataset: Dataset[_]): H2OWord2VecMOJOModel = {
    val inputCol: String = getInputCol()
    val ds = dataset
      .filter(col(inputCol).isNotNull)
      .filter(size(col(inputCol)) =!= 0)
      .withColumn(inputCol, explode(col(inputCol)))
      .select(inputCol)

    val model = super.fit(ds).asInstanceOf[H2OWord2VecMOJOModel]
    copyExtraParams(model)

    model
  }

  private[sparkling] def getInputCols(): Array[String] = Array(getInputCol())

  override def getColumnsToString(): Array[String] = getInputCols()

  private[sparkling] def setInputCols(cols: Array[String]) = {
    require(cols.length == 1, "Word2Vec supports only one input column")
    setInputCol(cols.head)
  }

}
