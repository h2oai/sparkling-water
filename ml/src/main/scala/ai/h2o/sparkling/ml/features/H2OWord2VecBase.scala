package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.models.H2OWord2VecMOJOModel
import ai.h2o.sparkling.ml.params.H2OWord2VecExtraParams
import hex.Model
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, size, udf}

import scala.reflect.ClassTag

abstract class H2OWord2VecBase[P <: Model.Parameters: ClassTag]
  extends H2OFeatureEstimator[P]
  with H2OWord2VecExtraParams {

  override def fit(dataset: Dataset[_]): H2OWord2VecMOJOModel = {
    validate(dataset.schema)
    val appendSentenceDelimiter: UserDefinedFunction = udf[Seq[String], Seq[String]](_ :+ "")
    val inputCol: String = getInputCol()
    val ds = dataset
      .filter(col(inputCol).isNotNull)
      .filter(size(col(inputCol)) =!= 0)
      .withColumn(inputCol, appendSentenceDelimiter(col(inputCol)))
      .withColumn(inputCol, explode(col(inputCol)))
      .select(inputCol)

    if (ds.isEmpty) {
      throw new IllegalArgumentException("Empty DataFrame as an input for the H2OWord2Vec is not supported.")
    }

    val model = super.fit(ds).asInstanceOf[H2OWord2VecMOJOModel]
    copyExtraParams(model)

    model
  }

  override def getColumnsToString(): Array[String] = getInputCols()

  private[sparkling] override def getInputCols(): Array[String] = Array(getInputCol())

  private[sparkling] override def setInputCols(cols: Array[String]) = {
    require(cols.length == 1, "Word2Vec supports only one input column")
    setInputCol(cols.head)
  }

}
