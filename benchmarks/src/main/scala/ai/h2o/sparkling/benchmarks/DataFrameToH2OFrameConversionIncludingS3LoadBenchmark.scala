package ai.h2o.sparkling.benchmarks

import org.apache.spark.sql.DataFrame

class DataFrameToH2OFrameConversionIncludingS3LoadBenchmark(context: BenchmarkContext)
  extends DataFrameToH2OFrameConversionBenchmark(context) {

  override protected def initialize(): DataFrame = loadRegularDataFrame()
}
