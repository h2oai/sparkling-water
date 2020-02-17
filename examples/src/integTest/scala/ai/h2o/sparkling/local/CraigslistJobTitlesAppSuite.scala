package ai.h2o.sparkling.local

import ai.h2o.sparkling.{IntegTestHelper, IntegTestStopper, LocalTest}
import ai.h2o.sparkling.examples.CraigslistJobTitlesApp
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CraigslistJobTitlesAppSuite extends FunSuite with IntegTestHelper {

  test("Launch Craigslist App Demo", LocalTest) {
    launch(CraigslistJobTitlesApp.getClass.getName.replace("$", ""),
      env {
        sparkMaster("local[*]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object CraigslistJobTitlesAppTest extends IntegTestStopper{
  def main(args: Array[String]): Unit = exitOnException{
    CraigslistJobTitlesApp.main(args)
  }
}