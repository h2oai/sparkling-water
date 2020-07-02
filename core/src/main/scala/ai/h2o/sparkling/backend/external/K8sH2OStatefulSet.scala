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

import java.util.concurrent.TimeUnit

import ai.h2o.sparkling.H2OConf
import io.fabric8.kubernetes.api.model.apps.{DoneableStatefulSet, StatefulSetFluent, StatefulSetSpecFluent}
import io.fabric8.kubernetes.api.model.{IntOrString, Pod, Quantity}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.spark.h2o.backends.internal.InternalH2OBackend.isClusterOfExpectedSize

import scala.collection.JavaConverters._

trait K8sH2OStatefulSet extends K8sUtils {

  protected def installH2OStatefulSet(client: KubernetesClient, conf: H2OConf, headlessServiceURL: String): String = {
    val set = client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .createOrReplaceWithNew()
      .withApiVersion("apps/v1")
      .withKind("StatefulSet")
    addMetadata(set, conf)
    addSpec(set, conf, headlessServiceURL)
    set.done()
    waitForClusterToBeReady(client, conf)
  }

  protected def deleteH2OStatefulSet(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OStatefulsetName)
      .delete()
    while (client
             .apps()
             .statefulSets()
             .inNamespace(conf.externalK8sNamespace)
             .withName(conf.externalK8sH2OStatefulsetName)
             .get() != null) {
      Thread.sleep(100)
    }
  }

  private def addMetadata(set: DoneableStatefulSet, conf: H2OConf): DoneableStatefulSet = {
    set
      .withNewMetadata()
      .withName(conf.externalK8sH2OStatefulsetName)
      .endMetadata()
  }

  private def addSpec(set: DoneableStatefulSet, conf: H2OConf, headlessServiceURL: String): DoneableStatefulSet = {
    val templateSpec = set
      .withNewSpec()
      .withServiceName(conf.externalK8sH2OServiceName)
      .withReplicas(conf.clusterSize.get.toInt)
      .withNewSelector()
      .withMatchLabels(convertLabelToMap(conf.externalK8sH2OLabel))
      .endSelector()
      .withNewTemplate()
      .withNewMetadata()
      .withLabels(convertLabelToMap(conf.externalK8sH2OLabel))
      .endMetadata()

    addTemplateSpec(templateSpec, conf, headlessServiceURL)
      .endTemplate()
      .endSpec()
  }

  private type TemplateSpec = StatefulSetSpecFluent.TemplateNested[StatefulSetFluent.SpecNested[DoneableStatefulSet]]

  private def addTemplateSpec(templateSpec: TemplateSpec, conf: H2OConf, headlessServiceURL: String): TemplateSpec = {
    templateSpec
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
      .withValue(headlessServiceURL)
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
  }

  private def waitForClusterToBeReady(client: KubernetesClient, conf: H2OConf): String = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OStatefulsetName)
      .waitUntilReady(conf.externalK8sServiceTimeout, TimeUnit.SECONDS)

    val start = System.currentTimeMillis()
    val timeout = conf.cloudTimeout
    while (System.currentTimeMillis() - start < timeout) {
      if (getPodsForStatefulSet(client, conf).length < conf.clusterSize.get.toInt
          && listReadyPods(client, conf).length != 1) {
        Thread.sleep(100)
      } else {
        return listReadyPods(client, conf).head.getMetadata.getName
      }
    }

    if (getPodsForStatefulSet(client, conf).length < conf.clusterSize.get.toInt
        && listReadyPods(client, conf).length != 1) {
      throw new RuntimeException("Timeout during clouding of external H2O backend on K8s.")
    } else {
      listReadyPods(client, conf).head.getMetadata.getName
    }

  }

  private def getPodsForStatefulSet(client: KubernetesClient, conf: H2OConf): Array[Pod] = {
    client
      .pods()
      .inNamespace(conf.externalK8sNamespace)
      .withLabels(convertLabelToMap(conf.externalK8sH2OLabel))
      .list()
      .getItems
      .asScala
      .filter(_.getStatus.getPhase == "Running")
      .toArray
  }

  private def listReadyPods(client: KubernetesClient, conf: H2OConf) = {
    getPodsForStatefulSet(client, conf).filter { pod =>
      val newPod = client
        .pods()
        .inNamespace(conf.externalK8sNamespace)
        .withName(pod.getMetadata.getName)
      newPod.isReady && newPod.getLog.contains(s"Created cluster of size ${conf.clusterSize.get}")
    }
  }
}
