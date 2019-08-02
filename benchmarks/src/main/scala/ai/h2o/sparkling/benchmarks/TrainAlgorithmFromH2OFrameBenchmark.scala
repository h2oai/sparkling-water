package ai.h2o.sparkling.benchmarks

import org.apache.spark.h2o.H2OFrame

class TrainAlgorithmFromH2OFrameBenchmark(context: BenchmarkContext, algorithmBundle: AlgorithmBundle)
  extends AlgorithmBenchmarkBase[H2OFrame](context, algorithmBundle) {

  override protected def initialize(): H2OFrame = loadDataToH2OFrame()

  override protected def body(trainingFrame: H2OFrame): Unit = {
    val h2oAlgorithm = algorithmBundle.h2oAlgorithm
    h2oAlgorithm._parms._train = trainingFrame.key
    h2oAlgorithm._parms._response_column = context.datasetDetails.labelCol
    h2oAlgorithm.trainModel().get()
  }
}
