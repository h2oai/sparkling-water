package ai.h2o.sparkling.local

import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import ai.h2o.sparkling.examples.HamOrSpamDemo
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HamOrSpamDemoSuite extends FunSuite with IntegTestHelper {

  test("Launch HamOrSpamDemo", LocalTest) {
    launch(HamOrSpamDemo.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object HamOrSpamDemoTest extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    HamOrSpamDemo.main(args)
  }
}