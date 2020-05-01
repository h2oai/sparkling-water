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

import java.io.{File, InputStream}

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkFiles
import org.apache.spark.expose.Utils

private[models] trait HasMojoData {

  private var mojoFileName: String = _

  // Called during init of the model
  def setMojoData(mojoData: InputStream, mojoName: String = "mojoData"): this.type = {
    val sparkSession = SparkSessionUtils.active
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(sparkSession.sparkContext.getConf))
    val mojoFile = File.createTempFile(mojoName, ".mojo", sparkTmpDir)
    FileUtils.copyInputStreamToFile(mojoData, mojoFile)
    mojoFileName = mojoFile.getName
    sparkSession.sparkContext.addFile(mojoFile.getAbsolutePath)
    mojoFile.delete()
    this
  }

  protected def getMojoLocalPath(): String = SparkFiles.get(mojoFileName)
}
