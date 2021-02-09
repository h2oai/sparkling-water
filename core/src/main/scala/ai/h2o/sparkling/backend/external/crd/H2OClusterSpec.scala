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

import io.fabric8.kubernetes.api.model.KubernetesResource

class H2OClusterSpec extends KubernetesResource {

  private var nodes: Int = 1
  private var customImage: H2OClusterCustomImage = null
  private var resources: H2OClusterResources = null

  def getNodes(): Int = nodes
  def getCustomImage(): H2OClusterCustomImage = customImage
  def getResources(): H2OClusterResources = resources

  def setNodes(value: Int): H2OClusterSpec = {
    this.nodes = value
    this
  }
  def setCustomImage(value: H2OClusterCustomImage): H2OClusterSpec = {
    this.customImage = value
    this
  }
  def setResources(value: H2OClusterResources): H2OClusterSpec = {
    this.resources = value
    this
  }

  override def toString(): String = {
    s"""H2OClusterSpec{
       |  nodes=$nodes,
       |  customImage=$customImage,
       |  resources=$resources
       |}""".stripMargin
  }
}
