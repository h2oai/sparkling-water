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

trait YARNIntegrationTest extends IntegrationTest {
  def launch(obj: Any): Unit = super.launch(obj, new IntegrationTestEnv {
    override def conf: Map[String, String] = super.conf ++ Map(
      "spark.yarn.max.executor.failures" -> "1", // In fail of executor, fail the test
      "spark.executor.instances" -> "6",
      "spark.executor.memory" -> "8G",
      "spark.driver.memory" -> "8G",
      "spark.executor.cores" -> "32",
      "spark.ext.h2o.hadoop.memory" -> "20G",
      "spark.ext.h2o.external.cluster.size" -> "2"
    )

    override val sparkMaster: String = "yarn-client"
  })
}
