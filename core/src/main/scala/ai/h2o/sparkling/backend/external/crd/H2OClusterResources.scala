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

class H2OClusterResources extends KubernetesResource {

  private var cpu: Int = 1
  private var memory: String = null
  private var memoryPercentage: Int = 90

  def getCpu(): Int = cpu
  def getMemory(): String = memory
  def getMemoryPercentage(): Int = memoryPercentage

  def setCpu(value: Int): H2OClusterResources = {
    this.cpu = value
    this
  }
  def setMemory(value: String): H2OClusterResources = {
    this.memory = value
    this
  }
  def setMemoryPercentage(value: Int): H2OClusterResources = {
    this.memoryPercentage = value
    this
  }

  override def toString(): String = {
    s"""H2OClusterResources{
       |  cpu=$cpu,
       |  memory=$memory,
       |  memoryPercentage=$memoryPercentage
       |}""".stripMargin
  }
}
