package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

class TrainAlgorithmFromDataFrameViaCsvConversionBenchmark(context: BenchmarkContext, algorithmBundle: AlgorithmBundle)
  extends AlgorithmBenchmarkBase[DataFrame, H2OFrame](context, algorithmBundle)
  with EstimatorCommonUtils {

  override protected def initialize(): DataFrame = loadDataToDataFrame()

  override protected def convertInput(input: DataFrame): H2OFrame = {
    val className = this.getClass.getSimpleName
    val destination = context.workingDir.resolve(className)
    input.write.mode(SaveMode.Overwrite).csv(destination.toString)
    H2OFrame(destination, input.columns)
  }

  override protected def train(trainingFrame: H2OFrame): H2OMOJOModel = {
    val (name, params) = algorithmBundle.h2oAlgorithm
    val newParams = params ++ Map(
      "training_frame" -> trainingFrame.frameId,
      "response_column" -> context.datasetDetails.labelCol)
    trainAndGetMOJOModel(s"/3/ModelBuilders/$name", newParams)
  }

  override protected def cleanUpData(dataFrame: DataFrame, frame: H2OFrame): Unit = {
    removeFromCache(dataFrame)
    frame.delete()
  }
}
