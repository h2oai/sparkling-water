package water.sparkling.itest

import org.apache.spark.deploy.SparkSubmit
import org.scalatest.{Tag, Suite}

/**
 * Integration test support to be run on top of Spark.
 */
trait SparkITest { self: Suite =>

  def launch(className: String, env: ITestEnv): Unit = {
    val cmdLine = Array[String](
      "--class", className,
      "--jars", env.swassembly,
      "--master", env.sparkMaster,
      env.testJar)
    SparkSubmit.main(cmdLine)
  }

  trait ITestEnv {
    lazy val swassembly = sys.props.getOrElse("sparkling.test.assembly",
      fail("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))

    lazy val testJar = sys.props.getOrElse("sparkling.test.jar",
      fail("The variable 'sparkling.test.jar' should point to a jar containing this file!"))

    lazy val sparkMaster = sys.props.getOrElse("MASTER",
      sys.props.getOrElse("spark.master",
        fail("The variable 'MASTER' should point to Spark cluster")))
  }

  object env {
    def apply(init: => Unit):ITestEnv = {
      val result = init
      new ITestEnv {}
    }
  }

  // Helper function to setup environment
  def sparkMaster(uri: String) = sys.props += (("spark.master", uri))
}

object YarnTest extends Tag("water.sparkling.itest.YarnTest")
object LocalTest extends Tag("water.sparkling.itest.LocalTest")
