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

import org.apache.spark.h2o.backends._
import water.{DKV, Key}

class H2OFrame(s: String) extends H2OFrameIFace {
  private val isInternalMode = H2OContext.ensure("Context should be running").getConf.runsInInternalClusterMode
  private val fr = if (isInternalMode) {
    new internal.H2OFrameImpl(s)
  } else {
    new external.H2OFrameImpl(s)
  }

  def this(key: Key[Frame]) = this(key.toString)

  def this(fr: water.fvec.Frame) = this(fr._key)

  def this(fr: water.fvec.H2OFrame) = this(fr._key)

  override val _key: Key[Frame] = fr._key
}

object H2OFrame {
  def findByKey(key: String): H2OFrame = {
    val isInternalMode = H2OContext.ensure("Context should be running").getConf.runsInInternalClusterMode
    if (isInternalMode) {
      val value = DKV.get(key)
      if (value == null) {
        null
      } else {
        value.className() match {
          case name if name.equals(classOf[water.fvec.Frame].getName) =>
            new H2OFrame(value.get[water.fvec.Frame]())
          case name if name.equals(classOf[water.fvec.H2OFrame].getName) =>
            new H2OFrame(value.get[water.fvec.H2OFrame]())
          case _ => throw new RuntimeException("Unsupported Frame type")
        }
      }
    } else {
      //TODO: check if the frame exists using rest api
      null
    }
  }

}