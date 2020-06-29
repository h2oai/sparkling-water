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
import io.fabric8.kubernetes.api.model.{IntOrString, PodList, Quantity}
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}

import scala.collection.JavaConverters._

trait KubernetesUtils {

  def stopExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    deleteH2OHeadlessService(client, conf)
    deleteH2OStatefulSet(client, conf)
  }

  def startExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    stopExternalH2OOnKubernetes(conf)
    installH2OHeadlessService(client, conf)
    installH2OStateFulSet(client, conf)
    conf.setH2OCluster(s"${getH2OHeadlessServiceURL(conf)}:54321")
  }

  def deleteH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OServiceName)
      .delete()
    while (H2OHeadlessServiceExist(client, conf)) {
      Thread.sleep(100)
    }
  }

  private def H2OHeadlessServiceExist(client: KubernetesClient, conf: H2OConf): Boolean = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .list()
      .getItems
      .asScala
      .exists(_.getMetadata.getName == conf.externalK8sH2OServiceName)
  }

  private def H2OStatefulSetExist(client: KubernetesClient, conf: H2OConf): Boolean = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .list()
      .getItems
      .asScala
      .exists(_.getMetadata.getName == conf.externalK8sH2OStatefulsetName)
  }

  def deleteH2OStatefulSet(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OStatefulsetName)
      .delete()
    while (H2OStatefulSetExist(client, conf)) {
      Thread.sleep(100)
    }
  }

  private def installH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .createOrReplaceWithNew()
      .withApiVersion("v1")
      .withKind("Service")
      .withNewMetadata()
      .withName(conf.externalK8sH2OServiceName)
      .endMetadata()
      .withNewSpec()
      .withType("ClusterIP")
      .withClusterIP("None")
      .withSelector(convertLabelToMap(conf.externalK8sH2OLabel).asJava)
      .addNewPort()
      .withProtocol("TCP")
      .withPort(54321)
      .endPort()
      .endSpec()
      .done()
    while (!H2OHeadlessServiceExist(client, conf)) {
      Thread.sleep(100)
    }
  }

  private def installH2OStateFulSet(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .createOrReplaceWithNew()
      .withApiVersion("apps/v1")
      .withKind("StatefulSet")
      .withNewMetadata()
      .withName(conf.externalK8sH2OStatefulsetName)
      .endMetadata()
      .withNewSpec()
      .withServiceName(conf.externalK8sH2OServiceName)
      .withReplicas(conf.clusterSize.get.toInt)
      .withNewSelector()
      .withMatchLabels(convertLabelToMap(conf.externalK8sH2OLabel).asJava)
      .endSelector()
      .withNewTemplate()
      .withNewMetadata()
      .withLabels(convertLabelToMap(conf.externalK8sH2OLabel).asJava)
      .endMetadata()
      .withNewSpec()
      .withTerminationGracePeriodSeconds(10.toLong)
      .addNewContainer()
      .withName(conf.externalK8sH2OServiceName)
      .withImage(conf.externalK8sDockerImage)
      .withNewResources()
      .addToRequests("memory", Quantity.parse(conf.externalMemory))
      .endResources()
      .addNewPort()
      .withContainerPort(54321)
      .withProtocol("TCP")
      .endPort()
      .withNewReadinessProbe()
      .withNewHttpGet()
      .withPath("/kubernetes/isLeaderNode")
      .withPort(new IntOrString(conf.externalK8sH2OApiPort))
      .endHttpGet()
      .withInitialDelaySeconds(5)
      .withPeriodSeconds(5)
      .withFailureThreshold(1)
      .endReadinessProbe()
      .addNewEnv()
      .withName("H2O_KUBERNETES_SERVICE_DNS")
      .withValue(getH2OHeadlessServiceURL(conf))
      .endEnv()
      .addNewEnv()
      .withName("H2O_NODE_LOOKUP_TIMEOUT")
      .withValue("180")
      .endEnv()
      .addNewEnv()
      .withName("H2O_NODE_EXPECTED_COUNT")
      .withValue(conf.clusterSize.get)
      .endEnv()
      .addNewEnv()
      .withName("H2O_KUBERNETES_API_PORT")
      .withValue(conf.externalK8sH2OApiPort.toString)
      .endEnv()
      .endContainer()
      .endSpec()
      .endTemplate()
      .endSpec()
      .done()
    waitForClusterToBeReady(client, conf)
  }

  private def waitForClusterToBeReady(client: KubernetesClient, conf: H2OConf): Unit = {
    while (!H2OStatefulSetExist(client, conf)) {
      Thread.sleep(100)
    }
    while (getPodsForStatefulSet(client, conf).getItems.size() < conf.clusterSize.get.toInt) {
      Thread.sleep(100)
    }
    while (listReadyPods(client, conf).size != 1) {
      Thread.sleep(100)
    }
  }

  def getPodsForStatefulSet(client: KubernetesClient, conf: H2OConf): PodList = {
    client
      .pods()
      .inNamespace(conf.externalK8sNamespace)
      .withLabels(convertLabelToMap(conf.externalK8sH2OLabel).asJava)
      .list()
  }

  def listReadyPods(client: KubernetesClient, conf: H2OConf) = {
    getPodsForStatefulSet(client, conf).getItems.asScala.filter { pod =>
      client
        .pods()
        .inNamespace(conf.externalK8sNamespace)
        .withName(pod.getMetadata.getName)
        .isReady
    }
  }

  private def exposeLeaderNodeExternally(conf: H2OConf): String = {

  }
  private def getH2OHeadlessServiceURL(conf: H2OConf): String = {
    s"${conf.externalK8sH2OServiceName}.${conf.externalK8sNamespace}.svc.${conf.externalK8sDomain}"
  }

  private def convertLabelToMap(label: String): Map[String, String] = {
    val split = label.split("=")
    Map(split(0) -> split(1))
  }

}
