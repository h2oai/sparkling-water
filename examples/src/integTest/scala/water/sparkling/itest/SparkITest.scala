package water.sparkling.itest

import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.repl.REPLClassServerUtils
import org.scalatest.{BeforeAndAfterEach, Tag, Suite}
import scala.collection.mutable

/**
 * Integration test support to be run on top of Spark.
 */
trait SparkITest extends BeforeAndAfterEach { self: Suite =>

  private var testEnv:IntegTestEnv = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testEnv = new TestEnvironment
  }

  override protected def afterEach(): Unit = {
    testEnv = null
    super.afterEach()
  }

  /** Launch given class name via SparkSubmit and use given environment
    * to configure SparkSubmit command line.
    * @param className name of class to launch as integration test
    * @param env Spark environment
    */
  def launch(className: String, env: IntegTestEnv): Unit = {
    val cmdLine = Seq[String](
      "--class", className,
      "--jars", env.swassembly,
      "--verbose",
      "--master", env.sparkMaster) ++
      env.sparkConf.get("spark.driver.memory").map(m => Seq("--driver-memory", m)).getOrElse(Nil) ++
      // Disable GA collection by default
      env.sparkConf.flatMap( p => Seq("--conf", s"${p._1}=${p._2}") ) ++
      Seq("--conf", "spark.ext.h2o.disable.ga=true") ++
      Seq("--conf", "spark.driver.extraJavaOptions=-XX:MaxPermSize=384m") ++
      Seq("--conf", "hdp.version="+env.hdpVersion) ++
      Seq("--conf", s"spark.ext.h2o.cloud.name=sparkling-water-${className.replace('.','-')}") ++
      Seq[String](env.testJar)

    if(!env.sparkMaster.startsWith("yarn")) {
      SparkSubmit.main(cmdLine.toArray)
    } else {
      // Launch it via command line
      import scala.sys.process._
      val sparkHome = System.getenv("SPARK_HOME")
      val cmdToLaunch = Seq( s"$sparkHome/bin/spark-submit") ++ cmdLine
      val proc = cmdToLaunch.!
      assert (proc == 0, "Process finished in wrong way!")
    }
  }

  object env {
    def apply(init: => Unit):IntegTestEnv = {
      val e = testEnv
      val result = init
      e
    }
  }

  trait IntegTestEnv {
    lazy val swassembly = sys.props.getOrElse("sparkling.test.assembly",
      fail("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))

    lazy val testJar = sys.props.getOrElse("sparkling.test.jar",
      fail("The variable 'sparkling.test.jar' should point to a jar containing this file!"))

    lazy val sparkMaster = sys.props.getOrElse("MASTER",
      sys.props.getOrElse("spark.master",
        fail("The variable 'MASTER' should point to Spark cluster")))

    lazy val hdpVersion = sys.props.getOrElse("sparkling.test.hdp.version",
      fail("The variable 'sparkling.test.hdp.version' is not set! It should containg version of hdp used"))

    def verbose:Boolean = true

    def sparkConf: mutable.Map[String, String]
  }

  private class TestEnvironment extends IntegTestEnv {
    val conf = mutable.HashMap.empty[String,String] += "spark.testing" -> "true"
    conf += "spark.repl.class.uri" -> REPLClassServerUtils.classServerUri
    override def sparkConf: mutable.Map[String, String] = conf
  }

  // Helper function to setup environment
  def sparkMaster(uri: String) = sys.props += (("spark.master", uri))

  def conf(key: String, value: String) = testEnv.sparkConf += key -> value
  def conf(key: String, value: Int):mutable.Map[String, String] = conf(key, value.toString)
}

// List of test tags - the intention is to use them for
// filtering.
object YarnTest extends Tag("water.sparkling.itest.YarnTest")
object LocalTest extends Tag("water.sparkling.itest.LocalTest")
object StandaloneTest extends Tag("water.sparkling.itest.StandaloneTest")
