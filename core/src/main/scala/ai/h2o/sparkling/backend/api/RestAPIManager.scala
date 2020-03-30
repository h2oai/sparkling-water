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

package ai.h2o.sparkling.backend.api

import java.util.ServiceLoader

import org.apache.spark.h2o.H2OContext
import water.api.RequestServer.DummyRestApiContext
import water.api.RestApiContext

private[api] class RestAPIManager(hc: H2OContext) {
  private val loader: ServiceLoader[RestApi] = ServiceLoader.load(classOf[RestApi])

  def registerAll(): Unit = {
    val dummyRestApiContext = new DummyRestApiContext
    // Register first the core
    register(CoreRestAPI, dummyRestApiContext)
    // Then additional APIs
    import scala.collection.JavaConverters._
    loader.reload()
    loader.asScala.foreach(api => register(api, dummyRestApiContext))
  }

  def register(api: RestApi, context: RestApiContext): Unit = {
    api.registerEndpoints(hc, context)
  }
}

object RestAPIManager {
  def apply(hc: H2OContext) = new RestAPIManager(hc)
}
