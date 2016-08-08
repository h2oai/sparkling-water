package water.sparkling.itest.local

import org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestStopper, LocalTest, IntegTestHelper}

@RunWith(classOf[JUnitRunner])
class AirlinesWithWeatherDemo2Suite extends FunSuite with IntegTestHelper {

  test("Launch AirlinesWithWeatherDemo2", LocalTest) {
    launch("water.sparkling.itest.local.AirlinesWithWeatherDemo2Test",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object AirlinesWithWeatherDemo2Test extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    AirlinesWithWeatherDemo2.main(args)
  }
}
