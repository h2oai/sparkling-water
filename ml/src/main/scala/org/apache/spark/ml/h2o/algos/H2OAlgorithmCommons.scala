package org.apache.spark.ml.h2o.algos

import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.ml.h2o.param.H2OCommonParams
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

/**
  * This trait contains methods that are shared across all algorithms.
  */
trait H2OAlgorithmCommons extends H2OCommonParams {
  protected def prepareDatasetForFitting(dataset: Dataset[_]): H2OFrame = {
    val excludedCols = getExcludedCols()

    // if this is left empty select
    if ($(featuresCols).isEmpty) {
      val features = dataset.columns.filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
      setFeaturesCols(features)
    }

    val cols = (getFeaturesCols() ++ excludedCols).map(col)
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    h2oContext.asH2OFrame(dataset.select(cols: _*).toDF())
  }
}
