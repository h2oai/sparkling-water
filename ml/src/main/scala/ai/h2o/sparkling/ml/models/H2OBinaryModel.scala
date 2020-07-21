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

package ai.h2o.sparkling.ml.models

import java.io.{DataInputStream, File, FileInputStream}
import java.nio.file.Files

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.BuildInfo
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import ai.h2o.sparkling.utils.{ScalaUtils, SparkSessionUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.expose.Logging
import water.AutoBuffer
import water.api.schemas3.ModelsV3

class H2OBinaryModel private (val modelId: String) extends HasBinaryModel with Logging with RestCommunication {

  def write(path: String): Unit = {
    val sc = SparkSessionUtils.active.sparkContext
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    withResource(fs.create(qualifiedOutputPath)) { out =>
      Files.copy(new File(getBinaryModel().get.getAbsolutePath).toPath, out)
    }
    logInfo(s"Binary Model Saved to: $qualifiedOutputPath")
  }
}

object H2OBinaryModel extends RestCommunication {

  def exists(modelId: String): Boolean = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val models = query[ModelsV3](endpoint, "/3/Models", conf)
    models.models.exists(_.model_id.name == modelId)
  }

  def read(path: String): H2OBinaryModel = {
    read(path, None)
  }

  private def extractVersionFromModel(path: String): String = {
    ScalaUtils.withResource(SparkSessionUtils.readHDFSFile(path)) { fis =>
      ScalaUtils.withResource(new DataInputStream(fis)) { dis =>
        // The number of 20 is chosen without any special meaning, but the purpose is the following:
        // We need to read the version from binary model and instead just reading the whole binary file,
        // we read the head.
        // The head of the file looks like:
        // 1 byte - persistence info
        // 1 byte - magic number
        // 1 up to 4 bytes - length of the string that follows ( H2O stores integer efficiently into smaller types when possible)
        // The version itself. The version 30.30.10.10 consist of 11 characters => 11 bytes
        val arr = new Array[Byte](20)
        dis.readFully(arr)
        val ab = new AutoBuffer(arr)
        val persistenceInfo = ab.get1U() // Byte indicates persistence info
        val magic = ab.get1U() // Magic byte
        if (persistenceInfo != 0x1C || magic != 0xED) {
          throw new RuntimeException("Invalid binary model")
        }
        ab.getStr
      }
    }
  }

  private def checkVersion(expected: String, modelVersion: String): Unit = {
    if (expected != modelVersion && !modelVersion.endsWith("99999")) {
      throw new IllegalArgumentException(s"""
           | The binary model has been trained in H2O of version
           | $modelVersion but you are currently running H2O version of $expected.
           | Please make sure that running Sparkling Water/H2O-3 cluster and the loaded binary
           | model correspond to the same H2O-3 version.""".stripMargin)
    }
  }

  private[sparkling] def read(path: String, modelIdOption: Option[String]): H2OBinaryModel = {
    verifyH2OIsRunning()
    val modelVersion = extractVersionFromModel(path)
    checkVersion(BuildInfo.H2OVersion, modelVersion)
    val modelId = if (modelIdOption.isDefined) {
      modelIdOption.get
    } else {
      val conf = H2OContext.ensure().getConf
      val endpoint = RestApiUtils.getClusterEndpoint(conf)
      val params = Map("dir" -> SparkSessionUtils.hdfsQualifiedPath(path), "model_id" -> "test")
      val modelsV3 = RestApiUtils.update[ModelsV3](endpoint, s"/99/Models.bin/", conf, params)
      modelsV3.models.head.model_id.name
    }
    val model = new H2OBinaryModel(modelId)

    model.setBinaryModel(SparkSessionUtils.readHDFSFile(path))
  }

  private def verifyH2OIsRunning(): Unit = {
    if (H2OContext.get().isEmpty) {
      throw new IllegalArgumentException("To use features available on a binary model, H2O Context has to be running!")
    }
  }
}
