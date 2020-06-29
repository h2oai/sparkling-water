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
import io.fabric8.kubernetes.api.model.{IntOrString, Pod, Quantity}
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}

import scala.collection.JavaConverters._

trait KubernetesUtils {

  def stopExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    deleteH2OHeadlessService(client, conf)
    deleteH2OStatefulSet(client, conf)
    if (conf.externalK8sExposeLeader) {
      deleteLeaderNodeExposeService(client, conf)
    }
  }

  def startExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val client = new DefaultKubernetesClient
    stopExternalH2OOnKubernetes(conf)
    installH2OHeadlessService(client, conf)
    val leaderPodName = installH2OStateFulSet(client, conf)
    if (conf.externalK8sExposeLeader) {
      installLeaderNodeExposeService(client, conf, leaderPodName)
      conf.setH2OCluster(getExposeServiceURL(client, conf))
    } else {
      conf.setH2OCluster(s"${getH2OHeadlessServiceURL(conf)}:54321")
    }
    Thread.sleep(10000)
  }

  private def deleteH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OServiceName)
      .delete()
    while (serviceExist(client, conf, conf.externalK8sH2OServiceName)) {
      Thread.sleep(100)
    }
  }

  private def serviceExist(client: KubernetesClient, conf: H2OConf, service: String): Boolean = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .list()
      .getItems
      .asScala
      .exists(_.getMetadata.getName == service)
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

  private def deleteH2OStatefulSet(client: KubernetesClient, conf: H2OConf): Unit = {
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
    while (!serviceExist(client, conf, conf.externalK8sH2OServiceName)) {
      Thread.sleep(100)
    }
  }

  private def installH2OStateFulSet(client: KubernetesClient, conf: H2OConf): String = {
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

  private def waitForClusterToBeReady(client: KubernetesClient, conf: H2OConf): String = {
    while (!H2OStatefulSetExist(client, conf)) {
      Thread.sleep(100)
    }
    while (getPodsForStatefulSet(client, conf).length < conf.clusterSize.get.toInt) {
      Thread.sleep(100)
    }
    while (listReadyPods(client, conf).length != 1) {
      Thread.sleep(100)
    }
    listReadyPods(client, conf).head.getMetadata.getName
  }

  def getPodsForStatefulSet(client: KubernetesClient, conf: H2OConf): Array[Pod] = {
    client
      .pods()
      .inNamespace(conf.externalK8sNamespace)
      .withLabels(convertLabelToMap(conf.externalK8sH2OLabel).asJava)
      .list()
      .getItems
      .asScala
      .filter(_.getStatus.getPhase == "Running")
      .toArray
  }

  def listReadyPods(client: KubernetesClient, conf: H2OConf) = {
    getPodsForStatefulSet(client, conf).filter { pod =>
      val newPod = client
        .pods()
        .inNamespace(conf.externalK8sNamespace)
        .withName(pod.getMetadata.getName)

      newPod.isReady
    }
  }

  def deleteLeaderNodeExposeService(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .withName(exposeLeaderServiceName(conf))
      .delete()
    while (serviceExist(client, conf, exposeLeaderServiceName(conf))) {
      Thread.sleep(100)
    }
  }

  private def installLeaderNodeExposeService(
      client: KubernetesClient,
      conf: H2OConf,
      leaderNodePodName: String): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .createOrReplaceWithNew()
      .withApiVersion("v1")
      .withKind("Service")
      .withNewMetadata()
      .withName(exposeLeaderServiceName(conf))
      .endMetadata()
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
      .done()
    while (!serviceExist(client, conf, conf.externalK8sH2OServiceName + "-expose-leader")) {
      Thread.sleep(100)
    }
    while (client
             .services()
             .inNamespace(conf.externalK8sNamespace)
             .withName(exposeLeaderServiceName(conf))
             .get()
             .getStatus
             .getLoadBalancer
             .getIngress
             .isEmpty) {
      Thread.sleep(100)
    }
  }

  private def getExposeServiceURL(client: KubernetesClient, conf: H2OConf): String = {
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

  private def getH2OHeadlessServiceURL(conf: H2OConf): String = {
    s"${conf.externalK8sH2OServiceName}.${conf.externalK8sNamespace}.svc.${conf.externalK8sDomain}"
  }

  private def convertLabelToMap(label: String): Map[String, String] = {
    val split = label.split("=")
    Map(split(0) -> split(1))
  }

  private def exposeLeaderServiceName(conf: H2OConf): String = {
    conf.externalK8sH2OServiceName + "-expose-leader"
  }
}
