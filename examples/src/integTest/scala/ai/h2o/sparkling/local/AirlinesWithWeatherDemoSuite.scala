package ai.h2o.sparkling.local

import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import ai.h2o.sparkling.examples.AirlinesWithWeatherDemo
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class AirlinesWithWeatherDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch AirlinesWithWeatherDemo", LocalTest) {
    launch(AirlinesWithWeatherDemo.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
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