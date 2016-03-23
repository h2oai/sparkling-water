package water.sparkling.scripts

import java.io.File

import org.apache.spark.repl.h2o.{CodeResults, H2OInterpreter}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Suite, BeforeAndAfterAll, FunSuite}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer


trait ScriptsTestHelper extends FunSuite with org.apache.spark.Logging with BeforeAndAfterAll {
  self: Suite =>
  var sparkConf: SparkConf = _
  var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    sc = new SparkContext(sparkConf)
    super.beforeAll()
  }


  override protected def afterAll(): Unit = {
    if(sc!=null){
      sc.stop()
    }
  }

  def defaultConf: SparkConf = {
    val assemblyJar = sys.props.getOrElse("sparkling.assembly.jar",
      fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))
    val conf = new SparkConf().setAppName("Script testing")
      .set("spark.repl.class.uri",H2OInterpreter.classServerUri)
      .set("spark.ext.h2o.repl.enabled","false") // disable repl in tests
      .set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=384m")
      .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=384m")
      .set("spark.driver.extraClassPath", assemblyJar)
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
        testResult.addTermValue(termName,loop.valueOfTerm(termName).get.toString)
    }

    testResult
  }

  def launchScript(scriptName: String, inspections: ScriptInspections = new ScriptInspections()): ScriptTestResult = {

    logInfo("\n\n\n\n\nLAUNCHING TEST FOR SCRIPT: " + scriptName + "\n\n\n\n\n")

    val sourceFile = new File("examples" + File.separator + "scripts" + File.separator + scriptName)

    val code = scala.io.Source.fromFile(sourceFile).mkString
    val loop = new H2OInterpreter(sc, sessionId = 1)
    val res = launch(code,loop, inspections)
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
