package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils

class TrainAlgorithmFromH2OFrameBenchmark(context: BenchmarkContext, algorithmBundle: AlgorithmBundle)
  extends AlgorithmBenchmarkBase[H2OFrame, H2OFrame](context, algorithmBundle)
  with EstimatorCommonUtils {

  override protected def initialize(): H2OFrame = loadDataToH2OFrame()

  override protected def convertInput(input: H2OFrame): H2OFrame = input

  override protected def train(trainingFrame: H2OFrame): H2OMOJOModel = {
    val (name, params) = algorithmBundle.h2oAlgorithm
    val newParams = params ++ Map(
      "training_frame" -> trainingFrame.frameId,
      "response_column" -> context.datasetDetails.labelCol)
    trainAndGetMOJOModel(s"/3/ModelBuilders/$name", newParams)
  }

  override protected def cleanUp(frame: H2OFrame, output: H2OMOJOModel): Unit = frame.delete()
}
