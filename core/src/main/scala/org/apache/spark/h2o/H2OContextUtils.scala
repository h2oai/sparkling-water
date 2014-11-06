package org.apache.spark.h2o

import java.io.File

import com.google.common.io.Files
import org.apache.spark.{SparkContext, SparkEnv}
import water.{H2OApp, H2O}

/**
 * Support methods.
 */
private[spark] object H2OContextUtils {

  /** Helper type expression a tuple of ExecutorId, IP, port */
  type NodeDesc = (String, String, Int)

  /** Generates and distributes a flatfile around Spark cluster.
    *
    * @param distRDD
    * @param basePort
    * @param incrPort
    * @return
    */
  def collectNodesInfo(distRDD: RDD[Int], basePort: Int, incrPort: Int): Array[NodeDesc] = {
    // Collect flatfile - tuple of (IP, port)
    val nodes = distRDD.map { index =>
      ( SparkEnv.get.executorId,
        java.net.InetAddress.getLocalHost.getAddress.map(_ & 0xFF).mkString("."),
        // FIXME: verify that port is available
        (basePort + incrPort*index))
    }.collect()
    nodes
  }

  def saveAsFile(content: String): File = {
    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    val flatFile = new File(tmpDir, "flatfile.txt")
    val p = new java.io.PrintWriter(flatFile)
    try {
      p.print(content)
    } finally {
      p.close()
    }
    flatFile
  }

  /**
   * Start H2O nodes on given executors.
   *
   * @param sc  Spark context
   * @param spreadRDD  helper RDD spread over all executors
   * @param executors list of tuples (executorId,ip,port) defining executors serving as a platform for
   *                  launching H2O services
   * @param h2oArgs arguments passed to H2O instances
   * @return return a tuple containing executorId and status of H2O node
   */
  def startH2O( sc: SparkContext,
                spreadRDD: RDD[Int],
                executors: Array[NodeDesc],
                h2oConf: H2OConf,
                h2oArgs: Array[String]): Array[(String,Boolean)] = {
    val executorIds = executors.map(_._1)
    val flatFile = if (h2oConf.useFlatFile)
                    Some(executors.map( en => s"${en._2}:${en._3}").mkString("\n"))
                   else
                    None

    spreadRDD.map { index =>  // RDD partition index
      // This executor
      val executorId = SparkEnv.get.executorId
      if (executorIds.contains(executorId)) {
        try {
          // Create a flatfile if required
          val launcherArgs = flatFile
                      .map(f => saveAsFile(f))
                      .map(f => h2oArgs ++ Array("-flatfile", f.getAbsolutePath))
                      .getOrElse(h2oArgs)
          // Do not launch H2O several times
          H2O.START_TIME_MILLIS.synchronized {
            if (H2O.START_TIME_MILLIS.get() == 0) {
              H2OApp.main(launcherArgs)
            }
          }
          (executorId, true)
        } catch {
          case e: Throwable => {
            e.printStackTrace()
            println(
              s""""Cannot start H2O node because: ${e.getMessage}
                 | h2o parameters: ${h2oArgs.mkString(",")}
               """.stripMargin)
            (executorId, false)
          }
        }
      } else {
        (executorId, false)
      }
    }.collect()
  }
}
