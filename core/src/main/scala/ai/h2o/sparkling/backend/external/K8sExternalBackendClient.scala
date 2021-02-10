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

import scala.collection.JavaConverters._

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.external.crd._
import io.fabric8.kubernetes.api.model.ObjectMeta
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, KubernetesClient}
import org.apache.spark.expose.Logging

trait K8sExternalBackendClient extends Logging {

  @transient private lazy val client = new DefaultKubernetesClient()

  def stopExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val crClient = customResourceClient(client, conf)
    val result = crClient
      .withName(conf.externalK8sH2OClusterName)
      .delete()
    if (!result) {
      throw new RuntimeException(s"Unsuccessful try to delete H2O cluster ${conf.externalK8sH2OClusterName}")
    }
  }

  def startExternalH2OOnKubernetes(conf: H2OConf): Unit = {
    val crClient = customResourceClient(client, conf)
    if (crClient == null) {
      throw new RuntimeException(
        s"Couldn't find custom resource definition for H2O-3 cluster, make sure H2O operator is running!")
    }
    val resource = spec(conf)
    crClient.create(resource)
    waitToBeReady(conf)
    conf.setH2OCluster(s"${getH2OHeadlessServiceURL(conf)}:54321")
  }

  private def waitToBeReady(conf: H2OConf): Unit = {
    var tries = 0
    var ready = false
    while (!ready && tries < conf.externalK8sH2OClusterTimeout) {
      val pods = client
        .pods()
        .inNamespace(conf.externalK8sNamespace)
        .withLabel("app", conf.externalK8sH2OClusterName)
        .list()
        .getItems
        .asScala
      val allRunning = pods.count(_.getStatus.getPhase == "Running") == conf.clusterSize.get.toInt
      val oneReady = pods
        .flatMap(_.getStatus.getConditions.asScala)
        .count(i => i.getType == "Ready" && i.getStatus == "True") == 1
      ready = allRunning && oneReady
      if (!ready) {
        Thread.sleep(1000)
        tries = tries + 1
      }
    }
    if (ready) {
      logInfo(s"H2O cluster is considered to be ready after $tries to check the satisfactory condition.")
    } else {
      throw new RuntimeException(s"H2O cluster is not considered to be ready after" +
        s"expiration of the deployment timeout (${conf.externalK8sH2OClusterTimeout} seconds).")
    }
  }

  protected def getH2OHeadlessServiceURL(conf: H2OConf): String = {
    s"${conf.externalK8sH2OClusterName}.${conf.externalK8sNamespace}.svc.${conf.externalK8sDomain}"
  }

  private def customResourceClient(client: KubernetesClient, conf: H2OConf) = {
    val crd = client.customResourceDefinitions().load(H2OCluster.definitionAsStream(conf)).get()
    client
      .customResources(crd, classOf[H2OCluster], classOf[H2OClusterList], classOf[H2OClusterDoneable])
      .inNamespace(conf.externalK8sNamespace)
  }

  private def spec(conf: H2OConf): H2OCluster = {
    val metadata = new ObjectMeta()
    metadata.setName(conf.externalK8sH2OClusterName)
    metadata.setNamespace(conf.externalK8sNamespace)

    val cluster = new H2OCluster()
    cluster.setApiVersion("h2o.ai/v1beta")
    cluster.setKind("H2O")
    cluster.setMetadata(metadata)
    cluster.setSpec(
      new H2OClusterSpec()
        .setNodes(conf.clusterSize.get.toInt)
        .setCustomImage(new H2OClusterCustomImage().setImage(conf.externalK8sDockerImage))
        .setResources(
          new H2OClusterResources()
            .setCpu(if (conf.nthreads < 1) 1 else conf.nthreads)
            .setMemory(conf.externalMemory)
            .setMemoryPercentage(100 - conf.externalExtraMemoryPercent)))
    cluster
  }
}
