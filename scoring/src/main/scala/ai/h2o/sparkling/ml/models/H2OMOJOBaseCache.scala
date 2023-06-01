package ai.h2o.sparkling.ml.models

import java.io.File

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.expose.Logging

import scala.collection.mutable

trait H2OMOJOBaseCache[B] extends Logging {

  private object Lock

  private val pipelineCache = mutable.Map.empty[String, B]
  private val lastAccessMap = mutable.Map.empty[String, Long]

  private lazy val cleanupRetryTimeout = {
    val sparkConf = SparkSessionUtils.active.sparkContext.getConf
    sparkConf.getInt("spark.ext.h2o.mojo.destroy.timeout", 10 * 60 * 1000)
  }

  private val cleanerThread = new Thread() {
    override def run(): Unit = {
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(cleanupRetryTimeout)
          Lock.synchronized {
            val toDestroy = lastAccessMap.flatMap {
              case (uid, lastAccess) =>
                val currentDiff = System.currentTimeMillis() - lastAccess
                if (currentDiff > cleanupRetryTimeout) {
                  logDebug(s"Removing mojo $uid from cache as it has not been used for $cleanupRetryTimeout ms.")
                  Some(uid)
                } else {
                  None
                }
            }
            toDestroy.map { uid =>
              lastAccessMap.remove(uid)
              pipelineCache.remove(uid)
            }
          }
        } catch {
          case _: InterruptedException => Thread.currentThread.interrupt()
        }
      }
    }
  }

  logDebug("Cleaner thread for unused MOJOs started.")

  def startCleanupThread(): Unit = Lock.synchronized {
    if (!cleanerThread.isAlive) {
      cleanerThread.setDaemon(true)
      cleanerThread.start()
    }
  }

  def getMojoBackend(uid: String, mojoGetter: () => File): B = getMojoBackend(uid, mojoGetter, Map.empty)

  def getMojoBackend(uid: String, mojoGetter: () => File, config: Map[String, Any]): B = Lock.synchronized {
    if (!pipelineCache.contains(uid)) {
      pipelineCache.put(uid, loadMojoBackend(mojoGetter(), config))
    }
    lastAccessMap.put(uid, System.currentTimeMillis())
    pipelineCache(uid)
  }

  def loadMojoBackend(mojo: File, config: Map[String, Any]): B
}
