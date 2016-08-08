package water.sparkling.itest.local

import org.apache.spark.examples.h2o.ProstateDemo
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestHelper, IntegTestStopper, LocalTest}

@RunWith(classOf[JUnitRunner])
class ProstateDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch ProstateDemo", LocalTest) {
    launch("water.sparkling.itest.local.ProstateDemoTest",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object ProstateDemoTest extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    ProstateDemo.main(args)
  }
}