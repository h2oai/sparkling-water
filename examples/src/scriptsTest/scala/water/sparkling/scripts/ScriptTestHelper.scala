package water.sparkling.scripts

import java.io.File
import java.net.InetAddress

import org.apache.spark.h2o.backends.SharedBackendConf
import org.apache.spark.h2o.backends.SharedBackendConf._
import org.apache.spark.h2o.backends.external.ExternalBackendConf
import org.apache.spark.h2o.{BackendIndependentTestHelper, FunSuiteWithLogging, H2OConf}
import org.apache.spark.repl.h2o.{CodeResults, H2OInterpreter}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import water.init.NetworkInit

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

trait ScriptsTestHelper extends FunSuiteWithLogging with BeforeAndAfterAll with BackendIndependentTestHelper {

  self: Suite =>
  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  lazy val assemblyJar = sys.props.getOrElse("sparkling.assembly.jar",
    fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))

  override protected def beforeAll(): Unit = {
    val cloudName = uniqueCloudName("scripts-tests")
    sparkConf.set(PROP_CLOUD_NAME._1, cloudName)
    sparkConf.set(PROP_CLIENT_IP._1, sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress))


    sparkConf.set(SharedBackendConf.PROP_CLIENT_IP._1,
      sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress))

    val cloudSize = 2
    sparkConf.set(ExternalBackendConf.PROP_EXTERNAL_H2O_NODES._1, cloudSize.toString)
    if(testsInExternalMode(sparkConf)){
      startCloud(cloudSize, cloudName, sparkConf.get("spark.ext.h2o.client.ip"), assemblyJar)
    }
    sc = new SparkContext(H2OConf.checkSparkConf(sparkConf))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    stopCloudIfExternal(sc)
    if (sc != null){
      sc.stop()
    }
    super.afterAll()
  }

  def defaultConf: SparkConf = {
    val conf = new SparkConf().setAppName("Script testing")
      .set("spark.ext.h2o.repl.enabled","false") // disable repl in tests
      .set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=384m")
      .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=384m")
      .set("spark.driver.extraClassPath", assemblyJar)
      .set("spark.scheduler.minRegisteredResourcesRatio", "1")
      .set("spark.task.maxFailures", "1") // Any task failures are suspicious
      .set("spark.rpc.numRetries", "1") // Any RPC failures are suspicious
      .set("spark.deploy.maxExecutorRetries", "1") // Do not restart executors
      .set("spark.ext.h2o.backend.cluster.mode", sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal"))
      .set("spark.ext.h2o.external.start.mode", sys.props.getOrElse("spark.ext.h2o.external.start.mode", "manual"))
      .setJars(Array(assemblyJar))
    conf
  }


  private def launch(code: String, loop: H2OInterpreter, inspections: ScriptInspections): ScriptTestResult = {
    val testResult = new ScriptTestResult()
    val codeExecutionStatus = loop.runCode(code)
    testResult.setCodeResult(codeExecutionStatus)
    println("\n\nInterpreter Response:\n" + loop.interpreterResponse +"\n")
    println("\n\nPrinted output:\n" + loop.consoleOutput +"\n")
    inspections.codeAndResponses.foreach{
      snippet => {
        val snippetExecutionStatus = loop.runCode(snippet)
        testResult.addSnippetResult(snippet,snippetExecutionStatus)
      }
    }

    inspections.termsAndValues.foreach {
      termName =>
        testResult.addTermValue(termName, loop.valueOfTerm(termName).get.toString)
    }

    testResult
  }

  def launchScript(scriptName: String, inspections: ScriptInspections = new ScriptInspections(), baseDirectoryName: String = "scripts"): ScriptTestResult = {

    logInfo("\n\n\n\n\nLAUNCHING TEST FOR SCRIPT: " + scriptName + "\n\n\n\n\n")

    val sourceFile = new File("examples" + File.separator + baseDirectoryName + File.separator + scriptName)

    val code = scala.io.Source.fromFile(sourceFile).mkString
    val loop = new H2OInterpreter(sc, sessionId = 1)
    val res = launch(code, loop, inspections)
    loop.closeInterpreter()
    res
  }

  def launchCode(code: String, inspections: ScriptInspections = new ScriptInspections()): ScriptTestResult = {
    logInfo("\n\n\n\n\nLAUNCHING CODE:\n" + code + "\n\n\n\n\n")

    val loop = new H2OInterpreter(sc, sessionId = 1)
    val res = launch(code,loop, inspections)
    loop.closeInterpreter()
    res
  }

  def launchCodeWithIntp(code: String, loop: H2OInterpreter, inspections: ScriptInspections = new ScriptInspections()): ScriptTestResult ={
    launch(code,loop,inspections)
  }

}
/**
  * Helper class which is used for script testing. We can specify name of terms and expected values and we can also
  * specify small code snippets which will be executed once the script has been interpreted. It is expected that any code
  * snippet is valid piece of scala code.
  *
  * First, the code snippets are executed and then the terms are checked
  */
class ScriptInspections {
  var termsAndValues = new ListBuffer[String]()
  var codeAndResponses = new ListBuffer[String]()

  def addTermToCheck(name: String) = {
    termsAndValues += name
  }

  def addSnippet(code: String) = {
    codeAndResponses += code
  }

}

class ScriptTestResult() {
  var realTermValues = new HashMap[String, String]()
  var snippetResults =  new ListBuffer[SnippetResult]
  var codeExecutionStatus = CodeResults.Success

  def setCodeResult(codeExecutionStatus: CodeResults.Value) = {
    this.codeExecutionStatus = codeExecutionStatus
  }
  def addSnippetResult(snippet: String, snippetExecutionStatus: CodeResults.Value) = {
    snippetResults+=SnippetResult(snippet, snippetExecutionStatus)
  }

  def addTermValue(termName: String, termValue: String) = {
    realTermValues+=(termName->termValue)
  }

}

case class SnippetResult(snippet: String, snippetExecutionStatus: CodeResults.Value)
