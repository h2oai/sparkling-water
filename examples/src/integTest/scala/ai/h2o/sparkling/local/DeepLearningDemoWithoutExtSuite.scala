package ai.h2o.sparkling.local

import ai.h2o.sparkling.examples.DeepLearningDemoWithoutExtension
import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeepLearningDemoWithoutExtSuite extends FunSuite with IntegTestHelper {

  test("Launch DeepLearningDemoWithoutExtension", LocalTest) {
    launch(DeepLearningDemoWithoutExtTest.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object DeepLearningDemoWithoutExtTest extends IntegTestStopper {
  def main(args: Array[String]): Unit = exitOnException {
    DeepLearningDemoWithoutExtension.main(args)
  }
}