/**
 * Tests for the scripts in the example/scripts directory
 */

package ai.h2o.sparkling

import ai.h2o.sparkling.repl.CodeResults
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeAppSmall extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("ChicagoCrimeAppSmall.script.scala") {
    val result = launchScript("ChicagoCrimeAppSmall.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptCraigListJobTitles extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("CraigslistJobTitles.script.scala ") {
    val result = launchScript("CraigslistJobTitles.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

/**
 * To run this test successfully we need to download the citibike-nyc data to
 * examples/bigdata/laptop/citibike-nyc directory
 */
@RunWith(classOf[JUnitRunner])
class CityBikeSharing extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("CityBikeSharing.script.scala") {
    val result = launchScript("CityBikeSharing.script.scala")
    var msg = "Problem during interpreting the script!"
    if (result.codeExecutionStatus == CodeResults.Exception) {
      msg = "Exception occurred during the execution. One possible cause could be missing necessary citibike-nyc data files in examples/bigdata/laptop/citibike-nyc/ folder."
    }
    assert(result.codeExecutionStatus == CodeResults.Success, msg)
  }
}
