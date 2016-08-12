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
package org.apache.spark.h2o.utils

import water.fvec.Vec

/**
 * Utilities to work with types and allows conversion from/to h2o types
 *
 */
object H2OTypeUtils {

  private[spark]
  def dataTypeToVecType(t: Class[_]): Byte = {
    t match {
      case q if q == classOf[java.lang.Byte] => Vec.T_NUM
      case q if q == classOf[java.lang.Short] => Vec.T_NUM
      case q if q == classOf[java.lang.Integer] => Vec.T_NUM
      case q if q == classOf[java.lang.Long] => Vec.T_NUM
      case q if q == classOf[java.lang.Float] => Vec.T_NUM
      case q if q == classOf[java.lang.Double] => Vec.T_NUM
      case q if q == classOf[java.lang.Boolean] => Vec.T_NUM
      case q if q == classOf[java.lang.String] => Vec.T_STR
      case q if q == classOf[java.sql.Timestamp] => Vec.T_TIME
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
