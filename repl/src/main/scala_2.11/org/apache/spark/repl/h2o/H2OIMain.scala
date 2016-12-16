package org.apache.spark.repl.h2o

import org.apache.spark.repl.Main
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain


/**
  * Slightly altered scala's IMain allowing multiple interpreters to coexist in parallel. Each line in repl
  * is wrapped in a package which name contains current session id
  */
private[repl] class H2OIMain private(initialSettings: Settings,
                                     interpreterWriter: IntpResponseWriter,
                                     val sessionID: Int)
  extends IMain(initialSettings, interpreterWriter) {

  /**
    * Ensure that each class defined in repl is in a package containing number of repl session
    */
  def setupClassNames() = {
    import naming._
    // sessionNames is lazy val and needs to be accessed first in order to be then set again to our desired value
    naming.sessionNames.line
    val fieldSessionNames = naming.getClass.getDeclaredField("sessionNames")
    fieldSessionNames.setAccessible(true)
    fieldSessionNames.set(naming, new SessionNames {
      override def line  = "intp_id_" + sessionID + "." + propOr("line")
    })
  }
}

object H2OIMain {

  val existingInterpreters = mutable.HashMap.empty[Int, H2OIMain]
  private var interpreterClassloader: InterpreterClassLoader = _
  private var _initialized = false

  private def setClassLoaderToSerializers(classLoader: ClassLoader): Unit = {
    SparkEnv.get.serializer.setDefaultClassLoader(classLoader)
    SparkEnv.get.closureSerializer.setDefaultClassLoader(classLoader)
  }

  private def initialize(sc: SparkContext): Unit = {
    if (Main.interp != null) {
      // Application has been started using SparkShell script.
      // Set the original interpreter classloader as the fallback class loader for all
      // class not defined in our custom REPL.
      interpreterClassloader = new InterpreterClassLoader(Main.interp.intp.classLoader)
    } else {
      // Application hasn't been started using SparkShell.
      // Set the context classloader as the fallback class loader for all
      // class not defined in our custom REPL
      interpreterClassloader = new InterpreterClassLoader(Thread.currentThread.getContextClassLoader)
    }
    setClassLoaderToSerializers(interpreterClassloader)
  }

  def getInterpreterClassloader: InterpreterClassLoader = {
    interpreterClassloader
  }


  def createInterpreter(sc: SparkContext, settings: Settings, interpreterWriter: IntpResponseWriter, sessionId: Int): H2OIMain = synchronized {
    if(!_initialized){
      initialize(sc)
      _initialized = true
    }
    existingInterpreters += (sessionId -> new H2OIMain(settings, interpreterWriter, sessionId))
    existingInterpreters(sessionId)
  }

  private lazy val _classOutputDirectory = {
    if (org.apache.spark.repl.Main.interp != null) {
      // Application was started using shell, we can reuse this directory
      org.apache.spark.repl.Main.outputDir
    } else {
      // REPL hasn't been started yet, create a new directory
      val conf = new SparkConf()
      val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
      val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
      outputDir
    }
  }

  def classOutputDirectory = _classOutputDirectory
}