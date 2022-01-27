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
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import org.apache.spark.expose.Logging

object K8sExternalBackendClient extends K8sHeadlessService with K8sH2OStatefulSet with Logging {

  def stopExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    deleteH2OHeadlessService(client, conf)
    deleteH2OStatefulSet(client, conf)
  }

  def startExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    stopExternalH2OOnKubernetes(conf)
    installH2OHeadlessService(client, conf)
    installH2OStatefulSet(client, conf, getH2OHeadlessServiceURL(conf))
    conf.setH2OCluster(s"${getH2OHeadlessServiceURL(conf)}:54321")
  }
}
