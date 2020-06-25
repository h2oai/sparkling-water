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
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.{V1APIServiceSpec, V1ObjectMeta, V1Service, V1ServicePort, V1ServiceSpec}
import io.kubernetes.client.util.Config

import scala.collection.JavaConverters._

trait KubernetesUtils {

  def setClient(): Unit = {
    val client = Config.defaultClient
    Configuration.setDefaultApiClient(client)
  }

  def deleteH2OHeadlessService(conf: H2OConf): Unit = {
    val api = new CoreV1Api()
    import io.kubernetes.client.openapi.models.V1DeleteOptions
    val v1DeleteOptions = new V1DeleteOptions
    v1DeleteOptions.setApiVersion("v1")
    api.deleteNamespacedService(
      conf.externalK8sH2OServiceName,
      conf.externalK8sNamespace,
      null,
      null,
      0,
      true,
      null,
      v1DeleteOptions)
  }

  def installH2OHeadlessService(conf: H2OConf): Unit = {
    val api = new CoreV1Api()

    val v1Metadata = new V1ObjectMeta
    v1Metadata.setName(conf.externalK8sH2OServiceName)

    val v1Spec = new V1ServiceSpec
    v1Spec.setType("ClusterIP")
    v1Spec.setClusterIP("None")
    v1Spec.setSelector(Map("app" -> conf.externalK8sH2OLabel).asJava)
    val v1Port = new V1ServicePort
    v1Port.setProtocol("TCP")
    v1Port.setPort(54321)
    v1Spec.setPorts(List(v1Port).asJava)

    val v1Service = new V1Service
    v1Service.setApiVersion("v1")
    v1Service.setKind("Service")
    v1Service.setMetadata(v1Metadata)
    v1Service.setSpec(v1Spec)
    api.createNamespacedService(conf.externalK8sNamespace, v1Service, null, null, null)
  }

  def startExternalH2OOnKuberentes(conf: H2OConf): Unit = {
    deleteH2OHeadlessService(conf)
    installH2OHeadlessService(conf)
  }
}
