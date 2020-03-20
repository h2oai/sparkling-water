package ai.h2o.sparkling.local

import ai.h2o.sparkling.examples.DeepLearningDemo
import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeepLearningDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch DeepLearnigDemo", LocalTest) {
    launch(DeepLearningDemo.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object DeepLearningDemoTest extends IntegTestStopper {
  def main(args: Array[String]): Unit = exitOnException {
    DeepLearningDemo.main(args)
  }
}
