package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.sql.DataFrame

class DataFrameToH2OFrameConversionBenchmark(context: BenchmarkContext)
  extends BenchmarkBase[DataFrame, H2OFrame](context) {

  override protected def initialize(): DataFrame = loadDataToDataFrame()

  override protected def body(dataFrame: DataFrame): H2OFrame = context.hc.asH2OFrame(dataFrame)

  override protected def cleanUp(dataFrame: DataFrame, h2oFrame: H2OFrame): Unit = {
    removeFromCache(dataFrame)
    h2oFrame.delete()
  }
}
