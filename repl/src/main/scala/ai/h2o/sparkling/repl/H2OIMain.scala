package ai.h2o.sparkling.repl

import org.apache.spark.SparkContext
import org.apache.spark.repl.Main

import scala.collection.mutable
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

/**
  * Slightly altered scala's IMain allowing multiple interpreters to coexist in parallel. Each line in repl
  * is wrapped in a package which name contains current session id
  */
private[repl] class H2OIMain private (initialSettings: Settings, interpreterWriter: IntpResponseWriter, sessionId: Int)
  extends IMain(initialSettings, interpreterWriter)
  with H2OIMainHelper {

  setupClassNames(naming, sessionId)
}

object H2OIMain extends H2OIMainHelper {

  val existingInterpreters = mutable.HashMap.empty[Int, H2OIMain]

  def createInterpreter(
      sc: SparkContext,
      settings: Settings,
      interpreterWriter: IntpResponseWriter,
      sessionId: Int): H2OIMain = synchronized {
    initializeClassLoader(sc)
    existingInterpreters += (sessionId -> new H2OIMain(settings, interpreterWriter, sessionId))
    existingInterpreters(sessionId)
  }

  private lazy val _classOutputDirectory = {
    if (Main.interp != null) {
      // Application was started using shell, we can reuse this directory
      Main.outputDir
    } else {
      // REPL hasn't been started yet, create a new directory
      newREPLDirectory()
    }
  }

  def classOutputDirectory = _classOutputDirectory
}
