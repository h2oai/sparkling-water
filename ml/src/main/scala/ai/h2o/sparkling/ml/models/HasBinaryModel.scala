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

import java.io.{File, FileInputStream, InputStream}

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.spark.SparkFiles

private[models] trait HasBinaryModel {

  private var binaryModelFileName: Option[String] = None

  private[sparkling] def setBinaryModel(model: InputStream): this.type =
    setBinaryModel(model, binaryModelName = "binaryModel")

  private[sparkling] def setBinaryModel(model: InputStream, binaryModelName: String): this.type = {
    val modelFile = SparkSessionUtils.inputStreamToTempFile(model, binaryModelName, ".bin")
    setBinaryModel(modelFile)
    this
  }

  private[sparkling] def setBinaryModel(model: File): this.type = {
    val sparkSession = SparkSessionUtils.active
    binaryModelFileName = Some(model.getName)
    if (getBinaryModel().isDefined && getBinaryModel().get.exists()) {
      // Copy content to a new temp file
      withResource(new FileInputStream(model)) { inputStream =>
        setBinaryModel(inputStream, binaryModelFileName.get)
      }
    } else {
      sparkSession.sparkContext.addFile(model.getAbsolutePath)
    }
    this
  }

  private[sparkling] def getBinaryModel(): Option[File] =
    binaryModelFileName.map(path => new File(SparkFiles.get(path)))
}
