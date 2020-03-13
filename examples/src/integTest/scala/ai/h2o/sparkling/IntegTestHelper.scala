package ai.h2o.sparkling

import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.SharedBackendConf
import org.scalatest.{BeforeAndAfterEach, Suite, Tag}
import water.init.NetworkInit

import scala.collection.mutable

/**
  * Integration test support to be run on top of Spark.
  */
trait IntegTestHelper extends BeforeAndAfterEach {
  self: Suite =>

  private var testEnv: IntegTestEnv = _

  /** Launch given class name via SparkSubmit and use given environment
    * to configure SparkSubmit command line.
    *
    * @param className name of class to launch as integration test
    * @param env       Spark environment
    */
  def launch(className: String, env: IntegTestEnv): Unit = {
    val cmdToLaunch = Seq[String](
      getSubmitScript(env.sparkHome),
      "--class", className,
      "--jars", env.assemblyJar,
      "--verbose",
      "--master", env.sparkMaster) ++
      env.sparkConf.get("spark.driver.memory").map(m => Seq("--driver-memory", m)).getOrElse(Nil) ++
      Seq("--conf", "spark.scheduler.minRegisteredResourcesRatio=1") ++
      Seq("--conf", "spark.ext.h2o.repl.enabled=false") ++ // disable repl in tests
      Seq("--conf", s"spark.test.home=${env.sparkHome}") ++
      Seq("--conf", "spark.task.maxFailures=1") ++ // Any task failures are suspicious
      Seq("--conf", "spark.rpc.numRetries=1") ++ // Any RPC failures are suspicious
      Seq("--conf", "spark.deploy.maxExecutorRetries=1") ++ // Fail directly, do not try to restart executors
      Seq("--conf", "spark.network.timeout=360s") ++ // Increase network timeout if jenkins machines are busy
      Seq("--conf", "spark.worker.timeout=360") ++ // Increase worker timeout if jenkins machines are busy
      // Need to disable timeline service which requires Jersey libraries v1, but which are not available in Spark2.0
      // See: https://www.hackingnote.com/en/spark/trouble-shooting/NoClassDefFoundError-ClientConfig/
      Seq("--conf", "spark.hadoop.yarn.timeline-service.enabled=false") ++
      Seq("--conf", s"spark.ext.h2o.external.start.mode=auto") ++
      Seq("--conf", s"spark.ext.h2o.backend.cluster.mode=${sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal")}") ++
      Seq("--conf", s"spark.ext.h2o.external.disable.version.check=true") ++
      env.sparkConf.flatMap(p => Seq("--conf", s"${p._1}=${p._2}")) ++
      Seq[String](env.itestJar)

    import scala.sys.process._
    val proc = cmdToLaunch.!
    assert(proc == 0, s"Process finished in wrong way! response=$proc from \n${cmdToLaunch mkString " "}")

  }

  def isYarnIntegTest() = {
    testEnv.sparkConf += ExternalBackendConf.PROP_EXTERNAL_CLUSTER_SIZE._1 -> "2"
  }

  // Helper function to setup environment
  def sparkMaster(uri: String) = sys.props += (("spark.master", uri))

  def conf(key: String, value: Int): mutable.Map[String, String] = conf(key, value.toString)

  def conf(key: String, value: String) = testEnv.sparkConf += key -> value

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testEnv = new TestEnvironment

    testEnv.sparkConf += SharedBackendConf.PROP_CLIENT_IP._1 ->
      sys.props.getOrElse("H2O_CLIENT_IP", NetworkInit.findInetAddressForSelf().getHostAddress)

    val cloudSize = 1
    testEnv.sparkConf += ExternalBackendConf.PROP_EXTERNAL_CLUSTER_SIZE._1 -> cloudSize.toString
  }

  override protected def afterEach(): Unit = {
    testEnv = null
    super.afterEach()
  }

  /** Determines whether we run on Windows or Unix and return correct spark-submit script location */
  private def getSubmitScript(sparkHome: String): String = {
    if (System.getProperty("os.name").startsWith("Windows")) {
      sparkHome + "\\bin\\spark-submit.cmd"
    } else {
      sparkHome + "/bin/spark-submit"
    }
  }

  trait IntegTestEnv {

    lazy val sparkHome = sys.props.getOrElse("SPARK_HOME",
      sys.props.getOrElse("spark.test.home",
        fail("None of both 'SPARK_HOME' and 'spark.test.home' variables is not set! It should point to Spark home directory.")))

    lazy val assemblyJar = sys.props.getOrElse("sparkling.assembly.jar",
      fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))

    lazy val itestJar = sys.props.getOrElse("sparkling.itest.jar",
      fail("The variable 'sparkling.itest.jar' should point to a jar containing integration test classes!"))

    lazy val sparkMaster = sys.props.getOrElse("MASTER",
      sys.props.getOrElse("spark.master",
        fail("None of both 'MASTER' and 'spark.master' variables is not set! It should specify Spark mode.")))

    def verbose: Boolean = true

    def sparkConf: mutable.Map[String, String]

    var isYarnIntegTest = false
  }

  private class TestEnvironment extends IntegTestEnv {
    val conf = mutable.HashMap.empty[String, String] += "spark.testing" -> "true"

    override def sparkConf: mutable.Map[String, String] = conf
  }

  object env {
    def apply(init: => Unit): IntegTestEnv = {
      val e = testEnv
      val result = init
      e
    }
  }

}

// List of test tags - the intention is to use them for
// filtering.
object YarnTest extends Tag("water.sparkling.itest.YarnTest")

object LocalTest extends Tag("water.sparkling.itest.LocalTest")


trait IntegTestStopper {

  def exitOnException(f: => Unit): Unit = {
    try {
      f
    } catch {
      case t: Throwable => {
        //logError("Test throws exception!", t)
        println("!!!! TEST THROWS EXCEPTION !!!!")
        t.printStackTrace()
        water.H2O.exit(-1)
      }
    }
  }
}