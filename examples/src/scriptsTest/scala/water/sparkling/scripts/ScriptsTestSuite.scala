/**
  * Tests for the scripts in the example/scripts directory
  */

package water.sparkling.scripts

import org.apache.spark.repl.CodeResults
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BasicInterpreterTests extends ScriptsTestHelper{

  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,1024]")
    super.beforeAll()
  }
  test("Code with exception") {
    val result = launchCode(h2OContext, "throw new Exception(\"Exception Message\")")
    assert(result.codeExecutionStatus==CodeResults.Exception, "Problem during interpreting the script!")
  }

  test("Incomplete code") {
    val result = launchCode(h2OContext, "val num = ")
    assert(result.codeExecutionStatus==CodeResults.Incomplete, "Code execution status should be Incomplete!")
  }

  test("Simple script which ends successfully") {
    val inspections = new ScriptInspections()
    inspections.addTermToCheck("num")

    val result = launchCode(h2OContext, "val num = 42", inspections)
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("num").get=="42","Value of term \"num\" should be 42")
  }
}


@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeHDFS extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4512]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  ignore("chicagoCrimeLarge.script.scala ") {
    val result = launchScript(h2OContext, "chicagoCrimeLarge.script.scala")
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeSmall extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,2048]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }
  test("chicagoCrimeSmall.script.scala ") {
    val result = launchScript(h2OContext, "chicagoCrimeSmall.script.scala")
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeSmallShell extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }
  test("chicagoCrimeSmallShell.script.scala") {
    val result = launchScript(h2OContext, "chicagoCrimeSmallShell.script.scala")
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptHamOrSpam extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }
  test("hamSpam.script.scala") {
    val inspections = new ScriptInspections()
    inspections.addSnippet("val answer1 = isSpam(\"Michal, h2oworld party tonight in MV?\", dlModel, hashingTF, idfModel, h2oContext)")
    inspections.addTermToCheck("answer1")
    inspections.addSnippet("val answer2 = isSpam(\"We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?\", dlModel, hashingTF, idfModel, h2oContext)")
    inspections.addTermToCheck("answer2")

    val result = launchScript(h2OContext, "hamOrSpam.script.scala",inspections)
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("answer1").get=="false","Value of term \"answer1\" should be false")
    assert(result.realTermValues.get("answer2").get=="true","Value of term \"answer2\" should be true")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptCraigListJobTitles extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }
  test("craigslistJobTitles.script.scala ") {
    val result = launchScript(h2OContext, "craigslistJobTitles.script.scala")
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
  }
}

/**
  * To run this test successfully we need to download the citibike-nyc data to
  * examples/bigdata/laptop/citibike-nyc directory
  */
@RunWith(classOf[JUnitRunner])
class ScriptStrata2015 extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  test("strata2015.script.scala") {
    val result = launchScript(h2OContext, "strata2015.script.scala")
    var msg = "Problem during interpreting the script!"
    if(result.codeExecutionStatus==CodeResults.Exception){
      msg = "Exception occurred during the execution. One possible cause could be missing necessary citibike-nyc data files in examples/bigdata/laptop/citibike-nyc/ folder."
    }
    assert(result.codeExecutionStatus==CodeResults.Success, msg)
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptStrataAirlines extends ScriptsTestHelper{
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }
  test("StrataAirlines.script.scala") {
    val result = launchScript(h2OContext, "StrataAirlines.script.scala")
    assert(result.codeExecutionStatus==CodeResults.Success, "Problem during interpreting the script!")
  }
}
