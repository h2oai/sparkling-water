package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.sql.{DataFrame, SaveMode}

class DataFrameToH2OFrameConversionViaCsvFilesBenchmark(context: BenchmarkContext)
  extends BenchmarkBase[DataFrame, H2OFrame](context) {

  override protected def initialize(): DataFrame = loadDataToDataFrame()

  override protected def body(dataFrame: DataFrame): H2OFrame = {
    val className = this.getClass.getSimpleName
    val destination = context.workingDir.resolve(className)
    dataFrame.write.mode(SaveMode.Overwrite).csv(destination.toString)
    H2OFrame(destination, dataFrame.columns)
  }

  override protected def cleanUp(dataFrame: DataFrame, h2oFrame: H2OFrame): Unit = {
    removeFromCache(dataFrame)
    h2oFrame.delete()
  }
}
