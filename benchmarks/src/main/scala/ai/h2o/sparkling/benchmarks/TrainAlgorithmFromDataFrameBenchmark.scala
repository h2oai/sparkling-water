package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.sql.DataFrame

class TrainAlgorithmFromDataFrameBenchmark(context: BenchmarkContext, algorithmBundle: AlgorithmBundle)
  extends AlgorithmBenchmarkBase[DataFrame, DataFrame](context, algorithmBundle) {

  override protected def initialize(): DataFrame = loadDataToDataFrame()

  override protected def convertInput(input: DataFrame): DataFrame = input

  override protected def train(trainingDataFrame: DataFrame): H2OMOJOModel = {
    val initializedAlgorithm = algorithmBundle.swAlgorithm.setLabelCol(context.datasetDetails.labelCol)
    initializedAlgorithm.fit(trainingDataFrame)
  }

  override protected def cleanUp(dataFrame: DataFrame, model: H2OMOJOModel): Unit = removeFromCache(dataFrame)
}
