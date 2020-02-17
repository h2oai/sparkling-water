package ai.h2o.sparkling.local

import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import ai.h2o.sparkling.examples.AirlinesWithWeatherDemo2
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AirlinesWithWeatherDemo2Suite extends FunSuite with IntegTestHelper {

  test("Launch AirlinesWithWeatherDemo2", LocalTest) {
    launch(AirlinesWithWeatherDemo2.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
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
