package ai.h2o.sparkling

import java.io.File

import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.repl.{CodeResults, H2OInterpreter}
import org.apache.spark.expose.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}
import water.init.NetworkInit

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer


trait ScriptsTestHelper extends FunSuite with Logging with BeforeAndAfterAll {

  self: Suite =>
  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  lazy val assemblyJar = sys.props.getOrElse("sparkling.assembly.jar",
    fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))

  override protected def beforeAll(): Unit = {
    sparkConf.set(SharedBackendConf.PROP_CLIENT_IP._1, sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress))

    sparkConf.set(SharedBackendConf.PROP_CLIENT_IP._1,
      sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress))

    val cloudSize = 1
    sparkConf.set(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_SIZE._1, cloudSize.toString)

    sc = new SparkContext(org.apache.spark.h2o.H2OConf.checkSparkConf(sparkConf))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {

    if (sc != null){
      sc.stop()
    }
    super.afterAll()
  }

  def defaultConf: SparkConf = {
    val conf = new SparkConf().setAppName("Script testing")
      .set("spark.ext.h2o.repl.enabled", "false") // disable repl in tests
      .set("spark.scheduler.minRegisteredResourcesRatio", "1")
      .set("spark.task.maxFailures", "1") // Any task failures are suspicious
      .set("spark.rpc.numRetries", "1") // Any RPC failures are suspicious
      .set("spark.deploy.maxExecutorRetries", "1") // Do not restart executors
      .set("spark.network.timeout", "360s") // Increase network timeout if jenkins machines are busy
      .set("spark.worker.timeout", "360") // Increase worker timeout if jenkins machines are busy
      .set("spark.ext.h2o.backend.cluster.mode", sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal"))
      .set("spark.ext.h2o.external.start.mode", "auto")
      .set("spark.ext.h2o.hadoop.memory", "3G")
      .set("spark.ext.h2o.external.disable.version.check", "true")
      .setJars(Array(assemblyJar))
    conf
  }


  private def launch(code: String, loop: H2OInterpreter, inspections: ScriptInspections): ScriptTestResult = {
    val testResult = new ScriptTestResult()
    val codeExecutionStatus = loop.runCode(code)
    testResult.setCodeResult(codeExecutionStatus)
    println("\n\nInterpreter Response:\n" + loop.interpreterResponse +"\n")
    println("\n\nPrinted output:\n" + loop.consoleOutput +"\n")
    inspections.codeAndResponses.foreach {
      snippet => {
        val snippetExecutionStatus = loop.runCode(snippet)
        testResult.addSnippetResult(snippet,snippetExecutionStatus)
      }
    }

    inspections.termsAndValues.foreach {
      termName =>
        testResult.addTermValue(termName, loop.extractValue(termName).map(_.toString).getOrElse("None"))
    }

    testResult
  }

  def launchScript(scriptName: String, inspections: ScriptInspections = new ScriptInspections(), baseDirectoryName: String = "scripts", prefixCode: String = ""): ScriptTestResult = {

    logInfo("\n\n\n\n\nLAUNCHING TEST FOR SCRIPT: " + scriptName + "\n\n\n\n\n")

    val sourceFile = new File("examples" + File.separator + baseDirectoryName + File.separator + scriptName)

    val code = prefixCode + "\n" + scala.io.Source.fromFile(sourceFile).mkString
    val loop = new H2OInterpreter(sc, sessionId = 1)
    val res = launch(code, loop, inspections)
    loop.closeInterpreter()
    res
  }

  def launchCode(code: String, inspections: ScriptInspections = new ScriptInspections()): ScriptTestResult = {
    logInfo("\n\n\n\n\nLAUNCHING CODE:\n" + code + "\n\n\n\n\n")

    val loop = new H2OInterpreter(sc, sessionId = 1)
    val res = launch(code, loop, inspections)
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
