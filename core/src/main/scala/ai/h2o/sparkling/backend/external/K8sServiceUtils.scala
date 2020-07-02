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

import io.fabric8.kubernetes.client.KubernetesClient

trait K8sServiceUtils {

  protected def waitForServiceToBeReady(
      client: KubernetesClient,
      namespace: String,
      serviceName: String,
      timeout: Int): Unit = {
    val start = System.currentTimeMillis()
    while (client
             .services()
             .inNamespace(namespace)
             .withName(serviceName)
             .get() == null) {
      if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) > timeout) {
        throw new RuntimeException(s"Timeout to start service '$serviceName' exceeded!")
      } else {
        Thread.sleep(100)
      }
    }
  }

  protected def deleteService(client: KubernetesClient, namespace: String, serviceName: String): Unit = {
    client
      .services()
      .inNamespace(namespace)
      .withName(serviceName)
      .delete()
  }
}
