package org.apache.spark.rdd

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{IntHolder, H2OContext}
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite
import water.fvec.DataFrame

/**
 * Testing schema for h2o schema rdd transformation.
 */
class H2OSchemaRDDTest extends FunSuite with SparkTestContext {

  sc = new SparkContext("local[*]", "test-local")
  hc = new H2OContext(sc).start()

  test("test creation of H2OSchemaRDD") {
    val h2oContext = hc
    import h2oContext._

    // FIXME: create different shapes of frame
    val dataFrame = new DataFrame(new File("examples/smalldata/prostate.csv"))
    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert(dataFrame.numRows() == schemaRdd.count(), "Number of lines in dataframe and in schema has to be same")
    dataFrame.delete()
  }

  test("test RDD to DataFrame to SchemaRDD way") {
    val h2oContext = hc
    import h2oContext._

    val rdd = sc.parallelize(1 to 10000, 1000).map(i => IntHolder(Some(i)))
    val dataFrame:DataFrame = rdd

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (rdd.count == dataFrame.numRows())
    assert (rdd.count == schemaRdd.count)
  }

}

object H2OSchemaRDDTest {
}
