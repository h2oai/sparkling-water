package water.sparkling.itest.local

import org.apache.spark.examples.h2o.HamOrSpamDemo
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest
import water.sparkling.itest.{IntegTestHelper, LocalTest}

@RunWith(classOf[JUnitRunner])
class HamOrSpamDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch HamOrSpamDemo", LocalTest) {
    launch("water.sparkling.itest.local.HamOrSpamDemoTest",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object HamOrSpamDemoTest extends itest.IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    HamOrSpamDemo.main(args)
  }
}