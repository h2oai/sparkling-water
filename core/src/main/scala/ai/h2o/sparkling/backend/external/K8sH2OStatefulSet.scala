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

import java.io.{ByteArrayInputStream, InputStream}

import ai.h2o.sparkling.H2OConf
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient

import scala.collection.JavaConverters._

trait K8sH2OStatefulSet extends K8sUtils {

  protected def installH2OStatefulSet(client: KubernetesClient, conf: H2OConf, headlessServiceURL: String): String = {
    val resource = client.load(spec(conf, headlessServiceURL)).get
    client.resourceList(resource).createOrReplace()
    waitForClusterToBeReady(client, conf)
  }

  protected def deleteH2OStatefulSet(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OStatefulsetName)
      .delete()
  }

  private def waitForClusterToBeReady(client: KubernetesClient, conf: H2OConf): String = {
    client
      .apps()
      .statefulSets()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OStatefulsetName)

    val start = System.currentTimeMillis()
    val timeout = conf.cloudTimeout
    while (System.currentTimeMillis() - start < timeout) {
      if (getPodsForStatefulSet(client, conf).length < conf.clusterSize.get.toInt
          || listReadyPods(client, conf).length != 1) {
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

  private def convertLabelToMap(label: String): java.util.Map[String, String] = {
    val split = label.split("=")
    Map(split(0) -> split(1)).asJava
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

  private def getNthreades(conf: H2OConf): Int = if (conf.nthreads > 0) conf.nthreads else 1

  private def spec(conf: H2OConf, headlessServiceURL: String): InputStream = {
    val spec = s"""
                  |apiVersion: apps/v1
                  |kind: StatefulSet
                  |metadata:
                  |  name: ${conf.externalK8sH2OStatefulsetName}
                  |  namespace: ${conf.externalK8sNamespace}
                  |spec:
                  |  serviceName: ${conf.externalK8sH2OServiceName}
                  |  podManagementPolicy: "Parallel"
                  |  replicas: ${conf.clusterSize.get}
                  |  selector:
                  |    matchLabels:
                  |      ${convertLabel(conf.externalK8sH2OLabel)}
                  |  template:
                  |    metadata:
                  |      labels:
                  |        ${convertLabel(conf.externalK8sH2OLabel)}
                  |    spec:
                  |      terminationGracePeriodSeconds: 10
                  |      containers:
                  |        - name: ${conf.externalK8sH2OServiceName}
                  |          image: '${conf.externalK8sDockerImage}'
                  |          resources:
                  |            requests:
                  |              cpu: ${getNthreades(conf)}
                  |              memory: "${conf.externalMemory}"
                  |            limits:
                  |              cpu: ${getNthreades(conf)}
                  |              memory: "${conf.externalMemory}"
                  |          ports:
                  |            - containerPort: 54321
                  |              protocol: TCP
                  |          readinessProbe:
                  |            httpGet:
                  |              path: /kubernetes/isLeaderNode
                  |              port: ${conf.externalK8sH2OApiPort}
                  |            initialDelaySeconds: 5
                  |            periodSeconds: 5
                  |            failureThreshold: 1
                  |          env:
                  |          - name: H2O_KUBERNETES_SERVICE_DNS
                  |            value: $headlessServiceURL
                  |          - name: H2O_NODE_LOOKUP_TIMEOUT
                  |            value: '180'
                  |          - name: H2O_NODE_EXPECTED_COUNT
                  |            value: '${conf.clusterSize.get}'
                  |          - name: H2O_KUBERNETES_API_PORT
                  |            value: '${conf.externalK8sH2OApiPort}'""".stripMargin
    new ByteArrayInputStream(spec.getBytes)
  }
}
