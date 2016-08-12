package water.sparkling.itest.local

import org.apache.spark.examples.h2o.CraigslistJobTitlesApp
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.sparkling.itest.{IntegTestHelper, IntegTestStopper, LocalTest}

@RunWith(classOf[JUnitRunner])
class CraigslistJobTitlesAppSuite extends FunSuite with IntegTestHelper {

  test("Launch Craigslist App Demo", LocalTest) {
    launch("water.sparkling.itest.local.CraigslistJobTitlesAppTest",
      env {
        sparkMaster("local-cluster[3,2,2048]")
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