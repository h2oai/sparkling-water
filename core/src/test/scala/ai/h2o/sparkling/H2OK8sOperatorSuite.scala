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
package ai.h2o.sparkling

import ai.h2o.sparkling.backend.external.K8sExternalBackendClient
import io.fabric8.kubernetes.client.DefaultKubernetesClient
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OK8sOperatorSuite extends FunSuite with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  object DummyClient extends K8sExternalBackendClient

  test("convertColumnsToCategorical with column names") {
    val h2oConf = H2OConf()
      .setClusterSize(3)
      .setExternalK8sDockerImage("h2oai/sparkling-water-external-backend:3.32.0.3-1-3.0")
      .setExternalK8sH2OApiPort(2)
    val client =  new DefaultKubernetesClient("https://0684CB86995C83A559928F00C18D4E46.gr7.us-west-2.eks.amazonaws.com")
//    Console.println(DummyOperator.isH2OOperatorRunning(client, h2oConf))
    DummyClient.startExternalH2OOnKubernetes(h2oConf)
//    Console.println(DummyOperator.isH2OOperatorRunning(client, h2oConf))
//    DummyClient.stopExternalH2OOnKubernetes(h2oConf)
//    Console.println(DummyOperator.isH2OOperatorRunning(client, h2oConf))
//    DummyCluster.deployH2OCluster(client, h2oConf)
  }
}
