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

package ai.h2o.sparkling.extensions.rest.api

import water.api.{AbstractRegister, RestApiContext}

class SparklingWaterRestApiHandlersRegister extends AbstractRegister {

  override def registerEndPoints(context: RestApiContext): Unit = {
    context.registerEndpoint(
      "sw_frame_initialize",
      "POST " + Paths.INITIALIZE_FRAME,
      classOf[ImportFrameHandler],
      "initialize",
      "Initializes a new frame before pushing chunks to the server")

    context.registerEndpoint(
      "sw_frame_finalize",
      "POST " + Paths.FINALIZE_FRAME,
      classOf[ImportFrameHandler],
      "finalize",
      "Performs finalizing procedures after the data chunks were delivered to the server")

    context.registerEndpoint(
      "sw_get_upload_plan",
      "GET " + Paths.UPLOAD_PLAN,
      classOf[ImportFrameHandler],
      "getUploadPlan",
      "Asks H2O to calculate upload plan for a given number of chunks.")

    context.registerEndpoint(
      "sw_set_log_level",
      "POST" + Paths.LOG_LEVEL,
      classOf[LogLevelHandler],
      "setLogLevel",
      "Set log level on H2O cluster"
    )

    context.registerEndpoint(
      "sw_get_log_level",
      "GET" + Paths.LOG_LEVEL,
      classOf[LogLevelHandler],
      "getLogLevel",
      "Get log level on H2O cluster"
    )
  }

  override def getName(): String = "Sparkling Water REST API Extensions"
}
