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

private[models] trait HasMojo {

  private var mojoFileName: String = _

  def setMojo(mojo: InputStream): this.type = setMojo(mojo, mojoName = "mojoData")

  def setMojo(mojo: InputStream, mojoName: String): this.type = {
    val mojoFile = SparkSessionUtils.inputStreamToTempFile(mojo, mojoName, ".mojo")
    setMojo(mojoFile)
    this
  }

  def setMojo(mojo: File): this.type = {
    val sparkSession = SparkSessionUtils.active
    mojoFileName = mojo.getName
    if (getMojo().exists()) {
      // Copy content to a new temp file
      withResource(new FileInputStream(mojo)) { inputStream =>
        setMojo(inputStream, mojoFileName)
      }
    } else {
      sparkSession.sparkContext.addFile(mojo.getAbsolutePath)
    }
    this
  }

  protected def getMojo(): File = new File(SparkFiles.get(mojoFileName))
}
