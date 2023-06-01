package ai.h2o.sparkling.backend.utils

import org.apache.spark.expose.Logging

trait ShellUtils extends Logging {
  protected def launchShellCommand(cmdToLaunch: Seq[String]): Int = {
    import scala.sys.process._
    val processOut = new StringBuffer()
    val processErr = new StringBuffer()

    val proc = cmdToLaunch
      .mkString(" ")
      .!(ProcessLogger({ msg =>
        processOut.append(msg + "\n")
        println(msg)
      }, { errMsg =>
        processErr.append(errMsg + "\n")
        println(errMsg)
      }))

    logInfo(processOut.toString)
    logError(processErr.toString)
    proc
  }
}
