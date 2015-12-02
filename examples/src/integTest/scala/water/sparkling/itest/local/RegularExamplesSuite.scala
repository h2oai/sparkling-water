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
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite1 extends FunSuite with SparkITest {

  test("Launch AirlinesWithWeatherDemo2", LocalTest) {
    launch( "org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite2 extends FunSuite with SparkITest {

  test("Launch DeepLearnigDemo", LocalTest) {
    launch( "org.apache.spark.examples.h2o.DeepLearningDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite3 extends FunSuite with SparkITest {

  test("Launch DeepLearningDemoWithoutExtension", LocalTest) {
    launch( "org.apache.spark.examples.h2o.DeepLearningDemoWithoutExtension",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite4 extends FunSuite with SparkITest {

  test("Launch ProstateDemo", LocalTest) {
    launch( "org.apache.spark.examples.h2o.ProstateDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite5 extends FunSuite with SparkITest {

  test("Launch HamOrSpam Demo", LocalTest) {
    launch( "org.apache.spark.examples.h2o.HamOrSpamDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite6 extends FunSuite with SparkITest {

  test("Launch Chicago Crime Demo", LocalTest) {
    launch( "org.apache.spark.examples.h2o.ChicagoCrimeAppSmall",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class RegularExamplesSuite7 extends FunSuite with SparkITest {

  test("Launch Craigslist App Demo", LocalTest) {
    launch( "org.apache.spark.examples.h2o.CraigslistJobTitlesApp",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}
