package org.apache.spark.h2o

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

  /**
   * Start H2O nodes on given executors.
   *
   * @param sc  Spark context
   * @param spreadRDD  helper RDD spread over all executors
   * @param executorIds executors which will be used to launch H2O
   * @param h2oArgs arguments passed to H2O instances
   * @return return a tuple containing executorId and status of H2O node
   */
  def startH2O(sc: SparkContext,
                       spreadRDD: RDD[Int],
                       executorIds: Array[String],
                       h2oArgs: Array[String]): Array[(String,Boolean)] = {
    spreadRDD.map { index =>
      // This executor
      val executorId = SparkEnv.get.executorId
      if (executorIds.contains(executorId)) {
        try {
          // Do not launch H2O several times
          H2O.START_TIME_MILLIS.synchronized {
            if (H2O.START_TIME_MILLIS.get() == 0) {
              H2OApp.main(h2oArgs)
            }
          }
          (executorId, true)
        } catch {
          case e: Throwable => {
            e.printStackTrace()
            println(s"Cannot start H2O node because: ${e.getMessage}")
            (executorId, false)
          }
        }
      } else {
        (executorId, false)
      }
    }.collect()
  }
}
