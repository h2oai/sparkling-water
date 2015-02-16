package water.api

import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Test method of RDDsHandler.
 */
@RunWith(classOf[JUnitRunner])
class RDDsHandlerSuite extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local")

  test("RDDsHandler.list() method") {
    val rname = "Test"
    val rpart = 21
    val rdd = sc.parallelize(1 to 10, rpart).setName(rname).cache()

    val rddsHandler = new RDDsHandler(sc)
    val result = rddsHandler.list(3, new RDDsV3)
    assert (result.rdds.length == 1, "Number of created and persisted RDDs should be 1")
    assert (result.rdds(0).name.equals(rname), "Name matches")
    assert (result.rdds(0).partitions == rpart, "Number of partitions matches")

  }
}
