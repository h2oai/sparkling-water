package ai.h2o.sparkling.benchmarks

import org.apache.spark.sql.DataFrame

class DataFrameToH2OFrameConversionViaCsvFilesIncludingS3LoadBenchmark(context: BenchmarkContext)
  extends DataFrameToH2OFrameConversionViaCsvFilesBenchmark(context) {

  override protected def initialize(): DataFrame = loadRegularDataFrame()
}
