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
import io.fabric8.kubernetes.api.model.{DoneableService, IntOrString}
import io.fabric8.kubernetes.client.KubernetesClient
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

trait K8sExposeLeaderService extends K8sServiceUtils {

  protected def getExposeLeaderServiceURL(client: KubernetesClient, conf: H2OConf): String = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .withName(exposeLeaderServiceName(conf))
      .get()
      .getStatus
      .getLoadBalancer
      .getIngress
      .asScala
      .head
      .getHostname
  }

  protected def installExposeLeaderService(client: KubernetesClient, conf: H2OConf, leaderNodePodName: String): Unit = {
    val service = client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .createOrReplaceWithNew()
      .withApiVersion("v1")
      .withKind("Service")
    addServiceMeta(service, conf)
    addServiceSpec(service, leaderNodePodName)
    service.done()
    waitForServiceToBeReady(
      client,
      conf.externalK8sNamespace,
      exposeLeaderServiceName(conf),
      conf.externalK8sServiceTimeout)

    val start = System.currentTimeMillis()
    while (client
             .services()
             .inNamespace(conf.externalK8sNamespace)
             .withName(exposeLeaderServiceName(conf))
             .get()
             .getStatus
             .getLoadBalancer
             .getIngress
             .isEmpty) {
      if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) > conf.externalK8sServiceTimeout) {
        throw new RuntimeException(
          s"Timeout for creation of service '${exposeLeaderServiceName(conf)}' has been reached.")
      } else {
        Thread.sleep(100)
      }
    }
  }

  protected def deleteExposeLeaderService(client: KubernetesClient, conf: H2OConf): Unit = {
    deleteService(client, conf.externalK8sNamespace, exposeLeaderServiceName(conf))
  }

  private def addServiceMeta(service: DoneableService, conf: H2OConf): DoneableService = {
    service
      .withNewMetadata()
      .withName(exposeLeaderServiceName(conf))
      .endMetadata()
  }

  private def addServiceSpec(service: DoneableService, leaderNodePodName: String): DoneableService = {
    service
      .withNewSpec()
      .withType("LoadBalancer")
      .withExternalTrafficPolicy("Local")
      .withSelector(Map("statefulset.kubernetes.io/pod-name" -> leaderNodePodName).asJava)
      .addNewPort()
      .withProtocol("TCP")
      .withPort(80)
      .withTargetPort(new IntOrString(54321))
      .endPort()
      .endSpec()
  }

  private def exposeLeaderServiceName(conf: H2OConf): String = {
    conf.externalK8sH2OServiceName + "-expose-leader"
  }
}
