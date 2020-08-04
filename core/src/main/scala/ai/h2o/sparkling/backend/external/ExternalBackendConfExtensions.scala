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

package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.external.ExternalBackendConf.PROP_EXTERNAL_DISABLE_VERSION_CHECK
import org.apache.spark.expose.Logging

/**
  * External backend configuration
  */
trait ExternalBackendConfExtensions extends SharedBackendConf with Logging {
  self: H2OConf =>

  def externalConfString: String =
    s"""Sparkling Water configuration:
       |  backend cluster mode : $backendClusterMode
       |  cluster start mode   : $clusterStartMode
       |  cloudName            : ${cloudName.getOrElse("Not set yet")}
       |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
       |  base port            : $basePort
       |  log level            : $logLevel
       |  nthreads             : $nthreads""".stripMargin

  private[backend] def isBackendVersionCheckDisabled =
    sparkConf.getBoolean(PROP_EXTERNAL_DISABLE_VERSION_CHECK._1, PROP_EXTERNAL_DISABLE_VERSION_CHECK._2)

}
