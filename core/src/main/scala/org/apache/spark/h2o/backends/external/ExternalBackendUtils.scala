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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.SharedBackendUtils
import org.apache.spark.h2o.utils.NodeDesc
import water.{ExternalFrameUtils, H2O}

/**
  * Various helper methods used in the external backend
  */
private[external] trait ExternalBackendUtils extends SharedBackendUtils {

  /**
    * Get arguments for H2O client
    *
    * @return array of H2O client arguments.
    */
  override def getH2OClientArgs(conf: H2OConf): Array[String] = {
    getH2OClientConnectionArgs(conf) ++ super.getH2OClientArgs(conf)
  }

  def cloudMembers = H2O.CLOUD.members().map(NodeDesc(_))

  private[this] def getH2OClientConnectionArgs(conf: H2OConf): Array[String] = {
    conf.h2oCluster.map(clusterStr => Array("-flatfile", saveAsFile(clusterStr).getAbsolutePath)).getOrElse(Array())
  }
}

object ExternalBackendUtils extends ExternalBackendUtils {

  def prepareExpectedTypes(classes: Array[Class[_]]): Array[Byte] = {
    classes.map { clazz =>
      if (clazz == classOf[java.lang.Boolean]) {
        ExternalFrameUtils.EXPECTED_BOOL
      } else if (clazz == classOf[java.lang.Byte]) {
        ExternalFrameUtils.EXPECTED_BYTE
      } else if (clazz == classOf[java.lang.Short]) {
        ExternalFrameUtils.EXPECTED_SHORT
      } else if (clazz == classOf[java.lang.Character]) {
        ExternalFrameUtils.EXPECTED_CHAR
      } else if (clazz == classOf[java.lang.Integer]) {
        ExternalFrameUtils.EXPECTED_INT
      } else if (clazz == classOf[java.lang.Long]) {
        ExternalFrameUtils.EXPECTED_LONG
      } else if (clazz == classOf[java.lang.Float]) {
        ExternalFrameUtils.EXPECTED_FLOAT
      } else if (clazz == classOf[java.lang.Double]) {
        ExternalFrameUtils.EXPECTED_DOUBLE
      } else if (clazz == classOf[java.lang.String]) {
        ExternalFrameUtils.EXPECTED_STRING
      } else if (clazz == classOf[java.sql.Timestamp]) {
        ExternalFrameUtils.EXPECTED_TIMESTAMP
      } else if (clazz == classOf[org.apache.spark.mllib.linalg.Vector]) {
        ExternalFrameUtils.EXPECTED_VECTOR
      } else {
        throw new RuntimeException("Unsupported class: " + clazz)
      }
    }
  }

}
