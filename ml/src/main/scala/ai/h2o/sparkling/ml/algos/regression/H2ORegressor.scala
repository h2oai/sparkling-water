package ai.h2o.sparkling.ml.algos.regression

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.H2OAlgoCommonUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

private[sparkling] trait H2ORegressor extends H2OAlgoCommonUtils {
  def getLabelCol(): String

  private def prepareDatasetForRegression(dataset: Dataset[_]): DataFrame = {
    val labelColumnName = getLabelCol()
    dataset.withColumn(labelColumnName, col(labelColumnName).cast(DoubleType))
  }

  override private[sparkling] def prepareDatasetForFitting(dataset: Dataset[_]): (H2OFrame, Option[H2OFrame]) = {
    super.prepareDatasetForFitting(prepareDatasetForRegression(dataset))
  }
}
