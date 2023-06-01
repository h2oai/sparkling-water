package ai.h2o.sparkling

import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Test methods available on H2OContext
  */
@RunWith(classOf[JUnitRunner])
class H2OContextTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test("setH2OLogLevel") {
    hc.setH2OLogLevel("DEBUG")
    assert(hc.getH2OLogLevel() == "DEBUG")
  }

  test("Sparkling Water communicates with leader node") {
    val clusterInfo = RestApiUtils.getClusterInfo(hc.getConf)
    val leaderIdx = clusterInfo.leader_idx
    assert(hc.getConf.h2oCluster.get == clusterInfo.nodes(leaderIdx).ip_port)
  }
}
