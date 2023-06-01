package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.H2OFrame

class H2OFrameToDataFrameConversionBenchmark(context: BenchmarkContext) extends BenchmarkBase[H2OFrame, Unit](context) {

  override protected def initialize(): H2OFrame = loadDataToH2OFrame()

  override protected def body(frame: H2OFrame): Unit = context.hc.asSparkFrame(frame).foreach(_ => {})

  override protected def cleanUp(frame: H2OFrame, none: Unit): Unit = frame.delete()
}
