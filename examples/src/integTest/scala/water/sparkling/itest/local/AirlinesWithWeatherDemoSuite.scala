package water.sparkling.itest.local

import org.apache.spark.examples.h2o.AirlinesWithWeatherDemo
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestStopper, LocalTest, IntegTestHelper}


@RunWith(classOf[JUnitRunner])
class AirlinesWithWeatherDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch AirlinesWithWeatherDemo", LocalTest) {
    launch("water.sparkling.itest.local.AirlinesWithWeatherDemoTest",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object AirlinesWithWeatherDemoTest extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    AirlinesWithWeatherDemo.main(args)
  }
}