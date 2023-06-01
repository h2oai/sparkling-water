package ai.h2o.sparkling.benchmarks

import org.apache.spark.sql.DataFrame

/**
  * The purpose of this benchmark is to measure how much time is spent on deserialization from data frame cache.
  */
class DummyDataFrameBenchmark(context: BenchmarkContext) extends BenchmarkBase[DataFrame, Unit](context) {

  override protected def initialize(): DataFrame = loadDataToDataFrame()

  override protected def body(dataFrame: DataFrame): Unit = dataFrame.count()

  override protected def cleanUp(dataFrame: DataFrame, none: Unit): Unit = removeFromCache(dataFrame)
}
