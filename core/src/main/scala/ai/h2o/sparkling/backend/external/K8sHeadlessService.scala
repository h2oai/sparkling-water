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
import io.fabric8.kubernetes.api.model.DoneableService
import io.fabric8.kubernetes.client.KubernetesClient

trait K8sHeadlessService extends K8sServiceUtils with K8sUtils {

  protected def getH2OHeadlessServiceURL(conf: H2OConf): String = {
    s"${conf.externalK8sH2OServiceName}.${conf.externalK8sNamespace}.svc.${conf.externalK8sDomain}"
  }

  protected def installH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    val service = client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .createOrReplaceWithNew()
      .withApiVersion("v1")
      .withKind("Service")
    addServiceMeta(service, conf)
    addServiceSpec(service, conf)
    service.done()
    waitForServiceToBeReady(
      client,
      conf.externalK8sNamespace,
      conf.externalK8sH2OServiceName,
      conf.externalK8sServiceTimeout)
  }

  protected def deleteH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    deleteService(client, conf.externalK8sNamespace, conf.externalK8sH2OServiceName)
  }

  private def addServiceSpec(service: DoneableService, conf: H2OConf): DoneableService = {
    service
      .withNewSpec()
      .withType("ClusterIP")
      .withClusterIP("None")
      .withSelector(convertLabelToMap(conf.externalK8sH2OLabel))
      .addNewPort()
      .withProtocol("TCP")
      .withPort(54321)
      .endPort()
      .endSpec()
  }

  private def addServiceMeta(service: DoneableService, conf: H2OConf): DoneableService = {
    service
      .withNewMetadata()
      .withName(conf.externalK8sH2OServiceName)
      .endMetadata()
  }
}
