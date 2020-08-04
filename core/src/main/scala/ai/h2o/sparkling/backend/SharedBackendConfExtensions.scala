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

package ai.h2o.sparkling.backend

import ai.h2o.sparkling.H2OConf

/**
  * Shared configuration independent on used backend
  */
trait SharedBackendConfExtensions {
  self: H2OConf =>

  import SharedBackendConf._

  private[backend] def getFileProperties: Seq[(String, _, _, _)] =
    Seq(PROP_JKS, PROP_LOGIN_CONF, PROP_SSL_CONF)

  protected def setBackendClusterMode(backendClusterMode: String) = {
    set(PROP_BACKEND_CLUSTER_MODE._1, backendClusterMode)
  }

  private[sparkling] def getClientLanguage: String = sparkConf.get(PROP_CLIENT_LANGUAGE._1, PROP_CLIENT_LANGUAGE._2)

}
