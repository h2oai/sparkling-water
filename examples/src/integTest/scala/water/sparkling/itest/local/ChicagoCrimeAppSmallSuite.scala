package water.sparkling.itest.local

import org.apache.spark.examples.h2o.ChicagoCrimeAppSmall
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestHelper, IntegTestStopper, LocalTest}

@RunWith(classOf[JUnitRunner])
class ChicagoCrimeAppSmallSuite extends FunSuite with IntegTestHelper {

  test("Launch Chicago Crime Demo", LocalTest) {
    launch("water.sparkling.itest.local.ChicagoCrimeAppSmallTest",
      env {
        sparkMaster("local-cluster[3,2,3072]")
        conf("spark.executor.memory", "3g")
        conf("spark.driver.memory", "3g")
      }
    )
  }
}

object ChicagoCrimeAppSmallTest extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    ChicagoCrimeAppSmall.main(args)
  }
}