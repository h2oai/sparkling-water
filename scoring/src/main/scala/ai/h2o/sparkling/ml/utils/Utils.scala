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

package ai.h2o.sparkling.ml.utils

import java.net.URL

import hex.genmodel.{ModelMojoReader, MojoModel, MojoReaderBackendFactory}

object Utils {

  /**
    * Get MOJO from a binary representation of MOJO
    *
    * @param mojoPath path to the MOJO model
    * @return MOJO model
    */
  def getMojoModel(mojoUrl: URL): MojoModel = {
    val reader = MojoReaderBackendFactory.createReaderBackend(mojoUrl, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    ModelMojoReader.readFrom(reader)
  }

  def getMojoModel(mojoPath: String): MojoModel = {
    val reader = MojoReaderBackendFactory.createReaderBackend(mojoPath)
    ModelMojoReader.readFrom(reader)
  }

}
