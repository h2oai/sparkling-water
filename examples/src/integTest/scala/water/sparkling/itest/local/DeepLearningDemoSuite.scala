package water.sparkling.itest.local

import org.apache.spark.examples.h2o.DeepLearningDemo
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestStopper, LocalTest, IntegTestHelper}

@RunWith(classOf[JUnitRunner])
class DeepLearningDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch DeepLearnigDemo", LocalTest) {
    launch("water.sparkling.itest.local.DeepLearningDemoTest",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object DeepLearningDemoTest extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    DeepLearningDemo.main(args)
  }
}