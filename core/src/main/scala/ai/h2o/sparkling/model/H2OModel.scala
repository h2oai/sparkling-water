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

package ai.h2o.sparkling.model

import java.io.File

import ai.h2o.sparkling.backend.external.{RestApiUtils, RestCommunication}
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.H2OContext

class H2OModel private(key: String) extends RestCommunication {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf

  private[sparkling] def downloadMOJOData(): Array[Byte] = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, this.key)
    downloadBinaryURLContent(endpoint, s"/3/Models/${this.key}/mojo", conf, target)
    import java.nio.file.Files
    Files.readAllBytes(target.toPath)
  }
}

object H2OModel {
  def apply(key: String) = new H2OModel(key)
}
