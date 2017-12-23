/**
  * Tests for the scripts in the example/scripts directory
  */

package water.sparkling.scripts

import org.apache.spark.repl.h2o.{H2OInterpreter, CodeResults}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BasicInterpreterTests extends ScriptsTestHelper {

  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf
      .setMaster("local-cluster[3,2,2048]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("Code with exception") {
    val result = launchCode("throw new Exception(\"Exception Message\")")
    assert(result.codeExecutionStatus == CodeResults.Exception, "Problem during interpreting the script!")
  }

  test("Incomplete code") {
    val result = launchCode("val num = ")
    assert(result.codeExecutionStatus == CodeResults.Incomplete, "Code execution status should be Incomplete!")
  }

  test("Simple script which ends successfully") {
    val inspections = new ScriptInspections()
    inspections.addTermToCheck("num")

    val result = launchCode("val num = 42", inspections)
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("num").get == "42", "Value of term \"num\" should be 42")
  }


  test("Test successful call after exception occurred") {
    val loop = new H2OInterpreter(sc, sessionId = 1)
    val result = launchCodeWithIntp("throw new Exception(\"Exception Message\")", loop)
    assert(result.codeExecutionStatus == CodeResults.Exception, "Problem during interpreting the script!")

    val result2 = launchCodeWithIntp("val a = 42", loop)
    assert(result2.codeExecutionStatus == CodeResults.Success, "Now it should end up as Success!")

    loop.closeInterpreter()
  }

  test("Test Spark API call via interpreter") {
    val inspections = new ScriptInspections()
    inspections.addTermToCheck("num1")
    inspections.addTermToCheck("num2")
    // FAILING: val num2 = sc.parallelize(Seq('A', 'B', 'A', 'C')).map(n => (n, 1)).reduceByKey(_ + _).count
    val result = launchCode(
      """
        |val list = Seq(('A', 1), ('B', 2), ('A', 3))
        |val num1 = sc.parallelize(list, 3).groupByKey.count
        |val num2 = sc.parallelize(list, 3).reduceByKey(_ + _).count
        |""".stripMargin, inspections)
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("num1").get == "2", "Value of term \"num\" should be 2")
    assert(result.realTermValues.get("num2").get == "2", "Value of term \"num\" should be 3")
  }

  test("[SW-386] Test Spark API exposed implicit conversions (https://issues.scala-lang.org/browse/SI-9734 and https://issues.apache.org/jira/browse/SPARK-13456)") {
    val inspections = new ScriptInspections()
    inspections.addTermToCheck("count")
    val result = launchCode(
      """
        |import spark.implicits._
        |case class Person(id: Long)
        |val ds = Seq(Person(0), Person(1)).toDS
        |val count = ds.count
      """.stripMargin, inspections)
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("count").get == "2", "Value of term \"count\" should be 2")
  }
}


@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeHDFS extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4512]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  ignore("chicagoCrimeLarge.script.scala ") {
    val result = launchScript("chicagoCrimeLarge.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeSmall extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,2048]")
      .set("spark.driver.memory", "2G")
      .set("spark.executor.memory", "2G")
    super.beforeAll()
  }

  test("chicagoCrimeSmall.script.scala ") {
    val result = launchScript("chicagoCrimeSmall.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptChicagoCrimeSmallShell extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  test("chicagoCrimeSmallShell.script.scala") {
    val result = launchScript("chicagoCrimeSmallShell.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptHamOrSpam extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
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
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  test("craigslistJobTitles.script.scala ") {
    val result = launchScript("craigslistJobTitles.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}

/**
  * To run this test successfully we need to download the citibike-nyc data to
  * examples/bigdata/laptop/citibike-nyc directory
  */
@RunWith(classOf[JUnitRunner])
class ScriptStrata2015 extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  test("strata2015.script.scala") {
    val result = launchScript("strata2015.script.scala")
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
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
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
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  test("hamOrSpamGBM.script.scala") {
    HamOrSpamTester.test(this, "hamOrSpamGBMMojo.script.scala")
  }
}

@RunWith(classOf[JUnitRunner])
class ScriptPipelineHamOrSpamDL extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3,2,4096]")
      .set("spark.driver.memory", "4G")
      .set("spark.executor.memory", "4G")
    super.beforeAll()
  }

  test("hamOrSpamDeepLearning.script.scala") {
    HamOrSpamTester.test(this, "hamOrSpamDeepLearningMojo.script.scala")
  }
}

object HamOrSpamTester {

  def test(scriptsTestHelper: ScriptsTestHelper, fileName: String) {
    val inspections = new ScriptInspections()
    inspections.addSnippet("val answer1 = isSpam(\"Michal, h2oworld party tonight in MV?\", model)")
    inspections.addTermToCheck("answer1")
    inspections.addSnippet("val answer2 = isSpam(\"We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?\", model)")
    inspections.addTermToCheck("answer2")

    val result = scriptsTestHelper.launchScript(fileName, inspections, "pipelines")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
    assert(result.realTermValues.get("answer1").get == "false", "Value of term \"answer1\" should be false")
    assert(result.realTermValues.get("answer2").get == "true", "Value of term \"answer2\" should be true")
  }
}

@RunWith(classOf[JUnitRunner])
class TestSparkApiViaScript extends ScriptsTestHelper {
  override protected def beforeAll(): Unit = {
    sparkConf = defaultConf.setMaster("local-cluster[3, 3, 2048]")
    super.beforeAll()
  }

  test("tests/sparkApiTest.script.scala") {
    val result = launchScript("tests/sparkApiTest.script.scala")
    assert(result.codeExecutionStatus == CodeResults.Success, "Problem during interpreting the script!")
  }
}
