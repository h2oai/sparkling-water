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
class ScriptHamOrSpam extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("hamOrSpam.script.scala") {
    val inspections = new ScriptInspections()
    inspections.addSnippet("val answer1 = isSpam(\"Michal, h2oworld party tonight in MV?\", dlModel, hashingTF, idfModel, h2oContext)")
    inspections.addTermToCheck("answer1")
    inspections.addSnippet("val answer2 = isSpam(\"We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?\", dlModel, hashingTF, idfModel, h2oContext)")
    inspections.addTermToCheck("answer2")

    val result = launchScript("hamOrSpam.script.scala", inspections)
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("answer1").get == "false", "Value of term \"answer1\" should be false")
    assert(result.realTermValues.get("answer2").get == "true", "Value of term \"answer2\" should be true")
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

@RunWith(classOf[JUnitRunner])
class ScriptStrataAirlines extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("StrataAirlines.script.scala") {
    val result = launchScript("StrataAirlines.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptPipelineHamOrSpamGBM extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("Ham or Spam GBM Pipeline") {
    HamOrSpamTester.test(this, "hamOrSpamMultiAlgo.script.scala", "gbm")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptPipelineHamOrSpamDL extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("Ham or Spam DeepLearning Pipeline") {
    HamOrSpamTester.test(this, "hamOrSpamMultiAlgo.script.scala", "dl")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptPipelineHamOrSpamXGBoost extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("Ham or Spam XGBoost Pipeline") {
    HamOrSpamTester.test(this, "hamOrSpamMultiAlgo.script.scala", "xgboost")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptPipelineHamOrSpamGridSearch extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("Ham or Spam Grid Search Pipeline") {
    HamOrSpamTester.test(this, "hamOrSpamMultiAlgo.script.scala", "grid_gbm")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptPipelineHamOrSpamAutoML extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local[*]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("Ham or Spam AutoML Pipeline") {
    HamOrSpamTester.test(this, "hamOrSpamMultiAlgo.script.scala", "automl")
  }
}

object HamOrSpamTester {

  def test(scriptsTestHelper: ScriptsTestHelper, fileName: String, algo: String) {
    val inspections = new ScriptInspections()
    inspections.addSnippet("val answer1 = isSpam(\"Michal, h2oworld party tonight in MV?\", loadedModel)")
    inspections.addTermToCheck("answer1")
    inspections.addSnippet("val answer2 = isSpam(\"We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?\", loadedModel)")
    inspections.addTermToCheck("answer2")

    val result = scriptsTestHelper.launchScript(fileName, inspections, "val algo=\"" + algo + "\"")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("answer1").get == "false", "Value of term \"answer1\" should be false")
    assert(result.realTermValues.get("answer2").get == "true", "Value of term \"answer2\" should be true")
  }
}
