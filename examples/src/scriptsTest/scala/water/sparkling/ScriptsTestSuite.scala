package water.scripts

import java.io._
import java.nio.file.StandardCopyOption

import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable


/**
 * Test for the scripts in the example/scripts directory
 */
@RunWith(classOf[JUnitRunner])
class ScriptsTestSuite extends FunSuite with ScriptsTestHelper with org.apache.spark.Logging {

  def launch(master: String,scriptPath: String, extraConf:mutable.Map[String, String]  = mutable.Map.empty ): TestResult ={
    val swfatjar = sys.props.getOrElse("sparkling.fat.jar",
      fail("The variable 'sparkling.fat.jar' is not set! It should point to assembly jar file."))

    val cmdLine = Seq[String](
      "--jars", swfatjar,
      "--verbose",
      "--master",master,
      "--driver-class-path",swfatjar) ++
      Seq("--conf", "spark.driver.extraJavaOptions=-XX:MaxPermSize=384m") ++
      extraConf.flatMap( p => Seq("--conf", s"${p._1}=${p._2}") ) ++
      Seq("-i",scriptPath)

    import sys.process._
    val sparkHome = System.getenv("SPARK_HOME")
    val cmdToLaunch = Seq( s"$sparkHome/bin/spark-shell") ++ cmdLine

    val out = new StringBuilder
    val err = new StringBuilder

    val logger = ProcessLogger(
      (o: String) => {
        out.append(o)
        logInfo(o)
      },
      (e: String) => {
        err.append(e)
        logError(e)
      }

    )

    val proc = cmdToLaunch.!(logger)
    // The last part of string is removed because it contains ERROR message:
    // ERROR FileAppender: Error writing stream to file. This is known spark bug
    // not solved yet, see https://issues.apache.org/jira/browse/SPARK-9844
    // This error however does not affect the functionality of scripts. It happens
    // at the end of each script after all commands are executed, so we can cut out the
    // part of output starting with this string so the tests won't fail
    val cutFrom = err.toString().indexOf("ERROR FileAppender")
    // cut the end only if the error actually occurred
    var errAltered = err.toString()
    if(cutFrom != -1){
      errAltered = err.toString().substring(0,cutFrom)
    }
    FileUtils.deleteQuietly(new File(scriptPath))
    new TestResult(proc,out.toString(),errAltered)
  }

  def launchScript(master: String,scriptName: String, extraConf:mutable.Map[String, String] = mutable.Map.empty): TestResult ={

    logInfo("\n\n\n\n\nLAUNCHING TEST FOR SCRIPT: "+scriptName+"\n\n\n\n\n")
    val sourceFile = new File("examples"+File.separator+"scripts"+File.separator+scriptName)
    val sourceFileWithExit = java.nio.file.Files.createTempFile("test",scriptName)
    java.nio.file.Files.copy(sourceFile.toPath,sourceFileWithExit,StandardCopyOption.REPLACE_EXISTING)
    val writer = new FileWriter(sourceFileWithExit.toFile,true)
    try{
      writer.write("\nexit")
    } finally writer.close()

    launch(master,sourceFileWithExit.toAbsolutePath.toString,extraConf)
  }

  def launchCode(master: String, code:String, extraConf:mutable.Map[String, String] = mutable.Map.empty): TestResult ={
    logInfo("\n\n\n\n\nLAUNCHING CODE:\n"+code+"\n\n\n\n\n")
    val sourceFile = java.nio.file.Files.createTempFile("test",".script.scala")
    val writer = new FileWriter(sourceFile.toFile,true)
    try{
      writer.write(code)
      writer.write("\nsc.stop")
      writer.write("\nexit")
    } finally writer.close()
    launch(master,sourceFile.toAbsolutePath.toString,extraConf)
  }

  test("Script with exception"){
    val result = launchCode("local-cluster[1,1,512]","throw new Exception(\"Exception Message\")")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.out)
    assertContainsIgnoreCase("exception",result.out)
    assertDoesNotContainIgnoreCase("exception",result.err)
    assertDoesNotContainIgnoreCase("error",result.err)
  }

  test("Script with error, class not imported"){
    val result = launchCode("local-cluster[1,1,512]","val h2oContext = new H2OContext(sc).start()")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertContainsIgnoreCase("error",result.out)
    assertDoesNotContainIgnoreCase("exception",result.out)
    assertDoesNotContainIgnoreCase("exception",result.err)
    assertDoesNotContainIgnoreCase("error",result.err)
  }

  // this test uses hdfs on one of the local h2o server which is not available from outside world
  ignore("chicagoCrime.script.scala ") {
    val extraConf = mutable.Map[String,String]()
    extraConf.put("spark.driver.memory","4G")
    val result = launchScript("local-cluster[3,2,4512]","chicagoCrime.script.scala",extraConf)
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

  test("chicagoCrimeSmall.script.scala ") {
    val result = launchScript("local-cluster[3,2,1024]","chicagoCrimeSmall.script.scala")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

  test("chicagoCrimeSmallShell.script.scala ") {
    val result = launchScript("local-cluster[3,2,4096]","chicagoCrimeSmallShell.script.scala")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

  test("mlconf_2015_hamSpam.script.scala") {
    val result = launchScript("local-cluster[3,2,4096]","mlconf_2015_hamSpam.script.scala")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

  test("craigslistJobTitles.script.scala") {
    val result = launchScript("local-cluster[3,2,4096]","craigslistJobTitles.script.scala")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

  test("strata2015.script.scala ") {
    val result = launchScript("local-cluster[3,2,4096]","strata2015.script.scala")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

  test("StrataAirlines.script.scala ") {
    val result = launchScript("local-cluster[3,2,4096]","StrataAirlines.script.scala")
    assert (result.exitStatus == 0, "Process finished in wrong way!")
    assertDoesNotContainIgnoreCase("error",result.err)
    assertDoesNotContainIgnoreCase("exception",result.err)
  }

}

trait ScriptsTestHelper{

  def assertContainsIgnoreCase(message:String, output: String) {
    assertContains(message.toLowerCase,output.toLowerCase)
  }

  def assertContains(message: String, output: String) {
    val isContain = output.contains(message)
    assert(isContain,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContainIgnoreCase(message:String, output: String) {
    assertDoesNotContain(message.toLowerCase,output.toLowerCase)
  }

  def assertDoesNotContain(message: String, output: String) {
    val isContain = output.contains(message)
    assert(!isContain,
      "Interpreter output contained '" + message + "':\n" + output)
  }

}
class TestResult(val exitStatus: Int, val out: String, val err:String){
}