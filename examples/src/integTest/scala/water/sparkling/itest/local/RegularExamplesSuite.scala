package water.sparkling.itest.local

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestHelper, LocalTest}

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
class AirlinesWithWeatherDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch AirlinesWithWeatherDemo", LocalTest) {
    launch("org.apache.spark.examples.h2o.AirlinesWithWeatherDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class AirlinesWithWeatherDemo2Suite extends FunSuite with IntegTestHelper {

  test("Launch AirlinesWithWeatherDemo2", LocalTest) {
    launch("org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class DeepLearnigDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch DeepLearnigDemo", LocalTest) {
    launch("org.apache.spark.examples.h2o.DeepLearningDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class DeepLearningDemoWithoutExtensionSuite extends FunSuite with IntegTestHelper {

  test("Launch DeepLearningDemoWithoutExtension", LocalTest) {
    launch("org.apache.spark.examples.h2o.DeepLearningDemoWithoutExtension",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class ProstateDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch ProstateDemo", LocalTest) {
    launch("org.apache.spark.examples.h2o.ProstateDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class HamOrSpamDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch HamOrSpamDemo", LocalTest) {
    launch("org.apache.spark.examples.h2o.HamOrSpamDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class ChicagoCrimeAppSmallSuite extends FunSuite with IntegTestHelper {

  test("Launch Chicago Crime Demo", LocalTest) {
    launch("org.apache.spark.examples.h2o.ChicagoCrimeAppSmall",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}

@RunWith(classOf[JUnitRunner])
class CraigslistJobTitlesAppSuite extends FunSuite with IntegTestHelper {

  test("Launch Craigslist App Demo", LocalTest) {
    launch("org.apache.spark.examples.h2o.CraigslistJobTitlesApp",
      env {
        sparkMaster("local-cluster[3,2,1024]")
        conf("spark.executor.memory", "1g")
        conf("spark.driver.memory", "1g")
      }
    )
  }
}
