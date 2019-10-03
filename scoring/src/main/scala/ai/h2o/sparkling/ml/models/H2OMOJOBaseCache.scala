/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package ai.h2o.sparkling.ml.models

import org.apache.spark.expose.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

trait H2OMOJOBaseCache[B, M] extends Logging {
  private object Lock

  private val pipelineCache = mutable.Map.empty[String, B]
  private val lastAccessMap = mutable.Map.empty[String, Long]

  private lazy val sparkConf = SparkSession.builder().getOrCreate().sparkContext.getConf
  private lazy val cleanupRetryTimeout = sparkConf.getInt("spark.ext.h2o.mojo.destroy.timeout",  10 * 60 * 1000)
  private val cleanerThread = new Thread() {
    override def run(): Unit = {
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(cleanupRetryTimeout)
          val toDestroy = lastAccessMap.flatMap { case (uid, lastAccess) =>
            val currentDiff = System.currentTimeMillis() - lastAccess
            if (currentDiff > cleanupRetryTimeout) {
              logDebug(s"Removing mojo $uid from cache as it has not been used for $cleanupRetryTimeout ms.")
              Some(uid)
            } else {
              None
            }
          }
          Lock.synchronized {
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


  def startCleanupThread(): Unit = {
    if (!cleanerThread.isAlive) {
      cleanerThread.start()
    }
  }

  def getMojoBackend(uid: String, bytes: Array[Byte], model: M): B = Lock.synchronized {
    if (!pipelineCache.contains(uid)) {
      pipelineCache.put(uid, loadMojoBackend(bytes, model))
    }
    lastAccessMap.put(uid, System.currentTimeMillis())
    pipelineCache(uid)
  }

  def loadMojoBackend(mojoData: Array[Byte], model: M): B
}
