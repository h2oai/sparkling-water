package water.sparkling.scripts

import java.io.File

import org.apache.spark.h2o.{SparklingConf, H2OContext}
import org.apache.spark.repl.{ClassLoaderHelper, H2OILoop, CodeResults}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Suite, BeforeAndAfterAll, FunSuite}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer


trait ScriptsTestHelper extends FunSuite with org.apache.spark.Logging with BeforeAndAfterAll {
  self: Suite =>
  var sparkConf: SparkConf = _
  var sc: SparkContext = _
  var h2OContext:H2OContext = _

  override protected def beforeAll(): Unit = {
    sc = new SparkContext(sparkConf)
    h2OContext = H2OContext.getOrCreate(sc)
    super.beforeAll()
  }


  override protected def afterAll(): Unit = {
    if(sc!=null){
      sc.stop()
    }
  }

  def defaultConf: SparkConf = {
    val assemblyJar = sys.props.getOrElse("sparkling.fat.jar",
      fail("The variable 'sparkling.fat.jar' is not set! It should point to assembly jar file."))
    val conf = new SparklingConf().setAppName("Script testing")
      .set("spark.driver.extraJavaOptions", "-XX:MaxPermSize=384m")
      .set("spark.executor.extraJavaOptions", "-XX:MaxPermSize=384m")
      .set("spark.driver.extraClassPath", assemblyJar)
      .setJars(Array(assemblyJar))

    conf
  }

  private def launch(h2oContext: H2OContext, code: String, inspections: ScriptInspections): ScriptTestResult = {
    val testResult = new ScriptTestResult()
    val loop = new H2OILoop(new ClassLoaderHelper(h2oContext.sparkContext), h2oContext.sparkContext, h2oContext, 1)
    val codeExecutionStatus = loop.runCode(code)
    testResult.setCodeResult(codeExecutionStatus)
    println("\n\nInterpreter Response:\n" + loop.interpreterResponse +"\n")
    println("\n\nPrinted output:\n" + loop.printedOutput +"\n")
    inspections.codeAndResponses.foreach{
      snippet => {
        val snippetExecutionStatus = loop.runCode(snippet)
        testResult.addSnippetResult(snippet,snippetExecutionStatus)
      }
    }

    inspections.termsAndValues.foreach {
      termName =>
        testResult.addTermValue(termName,loop.getInterpreter().valueOfTerm(termName).get.toString)
    }

    loop.closeInterpreter()
    testResult
  }

  def launchScript(h2oContext: H2OContext, scriptName: String, inspections: ScriptInspections = new ScriptInspections()): ScriptTestResult = {

    logInfo("\n\n\n\n\nLAUNCHING TEST FOR SCRIPT: " + scriptName + "\n\n\n\n\n")

    val sourceFile = new File("examples" + File.separator + "scripts" + File.separator + scriptName)
    val code = scala.io.Source.fromFile(sourceFile).mkString
    launch(h2oContext, code, inspections)
  }

  def launchCode(h2oContext: H2OContext, code: String, inspections: ScriptInspections = new ScriptInspections()): ScriptTestResult = {
    logInfo("\n\n\n\n\nLAUNCHING CODE:\n" + code + "\n\n\n\n\n")

    launch(h2oContext, code, inspections)
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
