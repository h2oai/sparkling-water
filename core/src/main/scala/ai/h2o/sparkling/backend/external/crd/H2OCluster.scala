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

package ai.h2o.sparkling.backend.external.crd

import java.io.{ByteArrayInputStream, InputStream}

import ai.h2o.sparkling.H2OConf
import io.fabric8.kubernetes.client.CustomResource

class H2OCluster extends CustomResource {

  private var spec: H2OClusterSpec = null

  def getSpec(): H2OClusterSpec = spec

  def setSpec(value: H2OClusterSpec): H2OCluster = {
    this.spec = value
    this
  }

  override def toString(): String = {
    s"""H2OCluster{
       |  apiVersion=${getApiVersion()},
       |  metadata=${getMetadata()},
       |  spec=$spec
       |}""".stripMargin
  }
}

object H2OCluster {
  def definition(conf: H2OConf): String =
    s"""apiVersion: apiextensions.k8s.io/v1
      |kind: CustomResourceDefinition
      |metadata:
      |  name: h2os.h2o.ai
      |  namespace: ${conf.externalK8sNamespace}
      |spec:
      |  group: h2o.ai
      |  names:
      |    kind: H2O
      |    plural: h2os
      |    singular: h2o
      |  scope: Namespaced
      |  version: v1beta
      |  versions:
      |    - name: v1beta
      |      served: true
      |      storage: true
      |      schema:
      |        openAPIV3Schema:
      |          type: object
      |          properties:
      |            spec:
      |              type: object
      |              properties:
      |                nodes:
      |                  type: integer
      |                version:
      |                  type: string
      |                customImage:
      |                  type: object
      |                  properties:
      |                    image:
      |                      type: string
      |                    command:
      |                      type: string
      |                  required: ["image"]
      |                resources:
      |                  type: object
      |                  properties:
      |                    cpu:
      |                      type: integer
      |                      minimum: 1
      |                    memory:
      |                      type: string
      |                      pattern: "^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$$"
      |                    memoryPercentage:
      |                      type: integer
      |                      minimum: 1
      |                      maximum: 100
      |                  required: ["cpu", "memory"]
      |              oneOf:
      |                - required: ["version"]
      |                - required: ["customImage"]
      |              required: ["nodes", "resources"]
      |""".stripMargin

  def definitionAsStream(conf: H2OConf): InputStream = new ByteArrayInputStream(definition(conf).getBytes)
}
