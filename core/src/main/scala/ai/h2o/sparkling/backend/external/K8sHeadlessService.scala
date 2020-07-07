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
import java.util.concurrent.TimeUnit

import ai.h2o.sparkling.H2OConf
import io.fabric8.kubernetes.client.KubernetesClient

trait K8sHeadlessService extends K8sUtils {

  protected def getH2OHeadlessServiceURL(conf: H2OConf): String = {
    s"${conf.externalK8sH2OServiceName}.${conf.externalK8sNamespace}.svc.${conf.externalK8sDomain}"
  }

  protected def installH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    val resource = client.load(spec(conf)).get
    client.resourceList(resource).createOrReplaceAnd().waitUntilReady(conf.externalK8sServiceTimeout, TimeUnit.SECONDS)
  }

  protected def deleteH2OHeadlessService(client: KubernetesClient, conf: H2OConf): Unit = {
    client
      .services()
      .inNamespace(conf.externalK8sNamespace)
      .withName(conf.externalK8sH2OServiceName)
      .delete()
  }

  private def spec(conf: H2OConf): InputStream = {
    val spec = s"""
      |apiVersion: v1
      |kind: Service
      |metadata:
      |  name: ${conf.externalK8sH2OServiceName}
      |  namespace: ${conf.externalK8sNamespace}
      |spec:
      |  type: ClusterIP
      |  clusterIP: None
      |  selector:
      |    ${convertLabel(conf.externalK8sH2OLabel)}
      |  ports:
      |  - protocol: TCP
      |    port: 54321""".stripMargin
    new ByteArrayInputStream(spec.getBytes)
  }
}
