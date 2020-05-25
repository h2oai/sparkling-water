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

package org.apache.spark.h2o

import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * Configuration holder which is representing
  * properties passed from user to Sparkling Water.
  */
class H2OConf(sparkConf: SparkConf) extends ai.h2o.sparkling.H2OConf(sparkConf) {
  this.set("spark.ext.h2o.rest.api.based.client", value = false)
  logWarning(
    "The class org.apache.spark.h2o.H2OConf is deprecated and will be removed in the version 3.34." +
      " Please use ai.h2o.sparkling.H2OConf instead.")
  def this() = this(SparkSessionUtils.active.sparkContext.getConf)
}

object H2OConf extends Logging {
  @DeprecatedMethod("ai.h2o.sparkling.H2OConf", "3.34")
  def apply(): H2OConf = new H2OConf()

  @DeprecatedMethod("ai.h2o.sparkling.H2OConf", "3.34")
  def apply(sparkConf: SparkConf): H2OConf = new H2OConf(sparkConf)

  private var _sparkConfChecked = false

  def sparkConfChecked = _sparkConfChecked

  def checkSparkConf(sparkConf: SparkConf): SparkConf = {
    _sparkConfChecked = true
    ai.h2o.sparkling.H2OConf.checkSparkConf(sparkConf)
  }
}
