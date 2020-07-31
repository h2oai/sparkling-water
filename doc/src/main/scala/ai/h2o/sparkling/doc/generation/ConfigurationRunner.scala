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

package ai.h2o.sparkling.doc.generation

import java.io.{File, PrintWriter}
import java.net.URLDecoder
import java.util.jar.JarFile

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.internal.InternalBackendConf
import ai.h2o.sparkling.utils.ScalaUtils.withResource

import scala.collection.mutable.ArrayBuffer

object ConfigurationRunner {
  private def writeResultToFile(content: String, fileName: String, destinationDir: String) = {
    val destinationDirFile = new File(destinationDir)
    destinationDirFile.mkdirs()
    val destinationFile = new File(destinationDirFile, s"$fileName.rst")
    withResource(new PrintWriter(destinationFile)) { outputStream =>
      outputStream.print(content)
    }
  }

  def main(args: Array[String]): Unit = {
    val destinationDir = args(0)
    writeResultToFile(ConfigurationsTemplate(), s"configuration_properties", destinationDir)
  }
}
