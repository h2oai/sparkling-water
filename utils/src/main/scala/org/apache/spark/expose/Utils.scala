package org.apache.spark.expose

import java.io.File

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.{ShutdownHookManager, Utils => SparkUtils}

object Utils {
  def createTempDir(root: String = System.getProperty("java.io.tmpdir"), namePrefix: String = "spark"): File = {
    SparkUtils.createTempDir(root, namePrefix)
  }

  def getLocalDir(conf: SparkConf): String = {
    SparkUtils.getLocalDir(conf)
  }

  def postToListenerBus(event: SparkListenerEvent): Unit = {
    SparkSessionUtils.active.sparkContext.listenerBus.post(event)
  }

  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(priority)(hook)
  }

  def removeShutdownHook(ref: AnyRef): Boolean = {
    ShutdownHookManager.removeShutdownHook(ref)
  }

  def inShutdown(): Boolean = ShutdownHookManager.inShutdown()
}
