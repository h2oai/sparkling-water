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

package ai.h2o.sparkling.backend.internal

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.SharedBackendConf

/**
  * Internal backend configuration
  */
trait InternalBackendConfExtensions extends SharedBackendConf {
  self: H2OConf =>

  import InternalBackendConf._

  def internalConfString: String =
    s"""Sparkling Water configuration:
       |  backend cluster mode : $backendClusterMode
       |  workers              : $numH2OWorkers
       |  cloudName            : ${cloudName.getOrElse(
         "Not set yet, it will be set automatically before starting H2OContext.")}
       |  base port            : $basePort
       |  cloudTimeout         : $cloudTimeout
       |  log level            : $logLevel
       |  nthreads             : $nthreads
       |  drddMulFactor        : $drddMulFactor""".stripMargin

  private[backend] override def getFileProperties: Seq[(String, _, _, _)] = super.getFileProperties :+ PROP_HDFS_CONF
}
