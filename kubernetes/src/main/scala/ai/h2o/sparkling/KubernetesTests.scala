package ai.h2o.sparkling

import org.apache.spark.sql.SparkSession

/**
  * Main class of the Kubernetes test suite. Invokes all Kubernetes tests.
  */
class KubernetesTests {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("App name").getOrCreate()
    val h2OContext = H2OContext.getOrCreate()
    val expected = spark.sparkContext.getConf.get("spark.executor.instances").toInt
    val actual = h2OContext.getH2ONodes().length
    if (actual != expected) {
      throw new RuntimeException(s"H2O cluster should be of size $expected but is $actual")
    }

    try {
      runTests(spark, h2OContext)
    } finally {
      h2OContext.stop(true)
    }

  }

  /**
    * Represents a whole suite of Kubernetes tests. Any test not invoked in this method is NOT ran.
    *
    * @param spark An instance of SparkSession
    * @param hc An instance of H2OContext
    */
  def runTests(spark: SparkSession, hc: H2OContext): Unit = {
    InitTest.run(spark, hc)
    DataFrameConversionTest.run(spark, hc)
  }
}
