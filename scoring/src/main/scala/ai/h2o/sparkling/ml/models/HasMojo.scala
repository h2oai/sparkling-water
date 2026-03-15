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

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.utils.ScalaUtils.withResource

private[models] trait HasMojo {

  private[sparkling] var mojoFileName: String = _

  // Store the raw MOJO bytes so the model can be used on executors without relying on
  // SparkContext.addFile(), which is broken in Spark 4 standalone mode (the file is not
  // distributed to already-running executors).
  private[sparkling] var mojoData: Array[Byte] = _

  // Per-JVM cached temp file rebuilt from mojoData when needed on an executor.
  @transient private lazy val localMojoFile: File = {
    val tmp = File.createTempFile(mojoFileName.stripSuffix(".mojo"), ".mojo")
    tmp.deleteOnExit()
    Files.write(tmp.toPath, mojoData)
    tmp
  }

  def setMojo(mojo: InputStream): this.type = setMojo(mojo, mojoName = "mojoData")

  def setMojo(mojo: InputStream, mojoName: String): this.type = {
    val buf = new ByteArrayOutputStream()
    val bytes = new Array[Byte](8192)
    var n = mojo.read(bytes)
    while (n != -1) { buf.write(bytes, 0, n); n = mojo.read(bytes) }
    mojoFileName = if (mojoName.endsWith(".mojo")) mojoName else mojoName + ".mojo"
    mojoData = buf.toByteArray
    this
  }

  def setMojo(mojo: File): this.type = {
    withResource(new FileInputStream(mojo)) { is =>
      val name = if (mojo.getName.endsWith(".mojo")) mojo.getName else mojo.getName + ".mojo"
      setMojo(is, name)
    }
  }

  private[sparkling] def getMojo(): File = localMojoFile
}
