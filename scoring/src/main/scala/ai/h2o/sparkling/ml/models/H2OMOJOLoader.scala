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

import java.io.InputStream

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.util.Identifiable

trait H2OMOJOLoader[T] {

  def createFromMojo(path: String): T = createFromMojo(path, H2OMOJOSettings.default)

  def createFromMojo(path: String, settings: H2OMOJOSettings): T = {
    val inputPath = new Path(path)
    createFromMojo(path, Identifiable.randomUID(inputPath.getName), settings)
  }

  def createFromMojo(path: String, uid: String, settings: H2OMOJOSettings): T
  /*
  = {
    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(SparkSessionUtils.active.sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    createFromMojo(is, Identifiable.randomUID(inputPath.getName), settings)
  }

  def createFromMojo(is: InputStream, uid: String): T = createFromMojo(is, uid, H2OMOJOSettings.default)

  def createFromMojo(is: InputStream, uid: String, settings: H2OMOJOSettings): T = {
    val byteArray = IOUtils.toByteArray(is)
    createFromMojo(byteArray, uid, settings)
  }

  def createFromMojo(mojoData: Array[Byte], uid: String): T = createFromMojo(mojoData, uid, H2OMOJOSettings.default)

  def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): T
 */
}
