package ai.h2o.sparkling.local

import ai.h2o.sparkling.examples.ProstateDemo
import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ProstateDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch ProstateDemo", LocalTest) {
    launch(ProstateDemo.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object ProstateDemoTest extends IntegTestStopper {
  def main(args: Array[String]): Unit = exitOnException {
    ProstateDemo.main(args)
  }
}