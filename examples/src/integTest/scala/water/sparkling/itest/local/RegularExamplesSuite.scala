package water.sparkling.itest.local

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{LocalTest, SparkITest}

/**
 * Test launcher for examples.
 *
 * We need to provide a separated suite for each H2O example
 * since H2O is not able to restart its context between
 * runs in the same JVM.
 *
 * Better solution would be use test tags to filter
 * them based on basic properties.
 */

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite extends FunSuite with SparkITest {

  test("Launch AirlinesWithWeatherDemo", LocalTest) {
    launch( "org.apache.spark.examples.h2o.AirlinesWithWeatherDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
      }
    )
  }
}
