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

import java.io.{ByteArrayOutputStream, File, FileInputStream, InputStream}
import java.nio.file.Files

import ai.h2o.sparkling.ml.utils.Utils
import ai.h2o.sparkling.utils.ScalaUtils.withResource

private[models] trait HasBinaryModel {

  private var binaryModelFileName: Option[String] = None
  private var binaryModelData: Option[Array[Byte]] = None

  @transient private lazy val localBinaryModelFile: Option[File] =
    binaryModelData.map { data =>
      val name = binaryModelFileName.getOrElse("binaryModel")
      Utils.mojoDataToTempFile(name, data)
    }

  private[sparkling] def setBinaryModel(model: InputStream): this.type =
    setBinaryModel(model, binaryModelName = "binaryModel")

  private[sparkling] def setBinaryModel(model: InputStream, binaryModelName: String): this.type = {
    val buf = new ByteArrayOutputStream()
    val bytes = new Array[Byte](8192)
    var n = model.read(bytes)
    while (n != -1) { buf.write(bytes, 0, n); n = model.read(bytes) }
    binaryModelFileName = Some(binaryModelName)
    binaryModelData = Some(buf.toByteArray)
    this
  }

  private[sparkling] def setBinaryModel(model: File): this.type = {
    withResource(new FileInputStream(model)) { is =>
      setBinaryModel(is, model.getName)
    }
  }

  private[sparkling] def getBinaryModel(): Option[File] = localBinaryModelFile
}
