package ai.h2o.sparkling.extensions.stacktrace

import java.util.Date

import water.{AbstractH2OExtension, H2O}
import water.util.Log


class StackTraceCollector extends AbstractH2OExtension {
  private var interval = -1 // -1 means disabled

  override def getExtensionName = "StackTraceCollector"

  override def printHelp(): Unit = {
    System.out.println(
      "\nStack trace collector extension:\n" +
        "    -stacktrace_collector_interval\n" +
        "          Time in seconds specifying how often to collect logs. \n"

    )
  }

  private def parseInterval(args: Array[String]): Array[String] = {
    for (i <- args.indices) {
      val s = new H2O.OptString(args(i))
      if (s.matches("stacktrace_collector_interval")) {
        interval = s.parseInt(args(i + 1))
        val new_args = new Array[String](args.length - 2)
        System.arraycopy(args, 0, new_args, 0, i)
        System.arraycopy(args, i + 2, new_args, i, args.length - (i + 2))
        return new_args
      }
    }
    args
  }

  override def parseArguments(args: Array[String]): Array[String] = parseInterval(args)

  override def onLocalNodeStarted(): Unit = {
    if (interval > 0) {
      new StackTraceCollectorThread().start()
    }
  }

  private class StackTraceCollectorThread extends Thread("StackTraceCollectorThread") {
    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        try {
          Log.info("Taking stacktrace at time: " + new Date)
          val allStackTraces = Thread.getAllStackTraces
          import scala.collection.JavaConversions._
          for (e <- allStackTraces.entrySet) {
            Log.info("Taking stacktrace for thread: " + e.getKey)
            for (st <- e.getValue) {
              Log.info("\t" + st.toString)
            }
          }
          Thread.sleep(interval * 1000)
        } catch {
          case _: InterruptedException =>
        }
      }
    }
  }

}
