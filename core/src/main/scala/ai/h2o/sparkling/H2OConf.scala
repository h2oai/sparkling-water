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

package ai.h2o.sparkling

import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.internal.InternalBackendConf
import ai.h2o.sparkling.repl.H2OInterpreter
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkConf
import org.apache.spark.expose.Logging

/**
  * Configuration holder which is representing
  * properties passed from user to Sparkling Water.
  */
class H2OConf(val sparkConf: SparkConf)
  extends Logging
  with InternalBackendConf
  with ExternalBackendConf
  with Serializable {

  if (!sparkConf.contains("spark.ext.h2o.client.language")) {
    sparkConf.set("spark.ext.h2o.client.language", "scala")
  }
  def this() = this(SparkSessionUtils.active.sparkContext.getConf)

  H2OConf.checkDeprecatedOptions(sparkConf)
  // Precondition
  require(sparkConf != null, "Spark conf was null")

  /** Copy this object */
  override def clone: H2OConf = {
    val conf = new H2OConf(sparkConf)
    conf.setAll(getAll)
    conf
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): H2OConf = {
    sparkConf.set(key, value)
    this
  }

  def set(key: String, value: Boolean): H2OConf = {
    sparkConf.set(key, value.toString)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): H2OConf = {
    sparkConf.remove(key)
    this
  }

  def contains(key: String): Boolean = sparkConf.contains(key)

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = sparkConf.get(key)

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = sparkConf.get(key, defaultValue)

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = sparkConf.getOption(key)

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    sparkConf.getAll
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): H2OConf = {
    sparkConf.setAll(settings)
    this
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = sparkConf.getInt(key, defaultValue)

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = sparkConf.getLong(key, defaultValue)

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = sparkConf.getDouble(key, defaultValue)

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = sparkConf.getBoolean(key, defaultValue)

  override def toString: String = {
    if (runsInExternalClusterMode) {
      externalConfString
    } else {
      internalConfString
    }
  }

  def getScheme(): String = {
    if (jks.isDefined && jksPass.isDefined) {
      "https"
    } else {
      "http"
    }
  }
}

object H2OConf extends Logging {
  def apply(): H2OConf = new H2OConf()

  def apply(sparkConf: SparkConf): H2OConf = new H2OConf(sparkConf)

  private val deprecatedOptions = Map[String, String](
    "spark.ext.h2o.node.iced.dir" -> "spark.ext.h2o.iced.dir",
    "spark.ext.h2o.client.iced.dir" -> "spark.ext.h2o.iced.dir",
    "spark.ext.h2o.client.log.level" -> "spark.ext.h2o.log.level",
    "spark.ext.h2o.node.log.level" -> "spark.ext.h2o.log.level",
    "spark.ext.h2o.client.flow.dir" -> "spark.ext.h2o.flow.dir",
    "spark.ext.h2o.client.port.base" -> "spark.ext.h2o.base.port",
    "spark.ext.h2o.node.port.base" -> "spark.ext.h2o.base.port",
    "spark.ext.h2o.node.log.dir" -> "spark.ext.h2o.log.dir",
    "spark.ext.h2o.client.log.dir" -> "spark.ext.h2o.log.dir",
    "spark.ext.h2o.node.extra" -> "spark.ext.h2o.extra.properties",
    "spark.ext.h2o.client.extra" -> "spark.ext.h2o.extra.properties")

  private def checkDeprecatedOptions(sparkConf: SparkConf): Unit = {
    deprecatedOptions.foreach {
      case (deprecated, current) =>
        val deprecatedValue = sparkConf.getOption(deprecated)
        if (deprecatedValue.isDefined) {
          val currentValue = sparkConf.getOption(current)
          if (currentValue.isDefined) {
            logWarning(
              s"Both options '$deprecated' and '$current' are specified. " +
                s"Using value '${currentValue.get}' of '$current' as the later one is deprecated.")
          } else {
            logWarning(
              s"Please use '$current' as '$deprecated' is deprecated. Passing the value '${deprecatedValue.get}' to '$current'.")
            sparkConf.set(current, deprecatedValue.get)
          }
        }
    }
  }

  private var _sparkConfChecked = false

  def sparkConfChecked = _sparkConfChecked

  def checkSparkConf(sparkConf: SparkConf): SparkConf = {
    _sparkConfChecked = true
    sparkConf.set("spark.repl.class.outputDir", H2OInterpreter.classOutputDirectory.getAbsolutePath)
    sparkConf
  }
}
