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

package org.apache.spark.ml.h2o.models

import java.io.InputStream

import ai.h2o.sparkling.macros.DeprecatedMethod
import org.apache.hadoop.fs.Path
import org.apache.spark.expose.Logging
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession

@Deprecated
trait H2OMOJOLoader[T] extends Logging {

  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOModel.createFromMojo")
  def createFromMojo(path: String): T = createFromMojo(path, H2OMOJOSettings.default)

  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOModel.createFromMojo")
  def createFromMojo(path: String, settings: H2OMOJOSettings): T = {
    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    createFromMojo(is, Identifiable.randomUID(inputPath.getName), settings)
  }

  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOModel.createFromMojo")
  def createFromMojo(is: InputStream, uid: String): T = createFromMojo(is, uid, H2OMOJOSettings.default)

  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOModel.createFromMojo")
  def createFromMojo(is: InputStream, uid: String, settings: H2OMOJOSettings): T = {
    createFromMojo(Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray, uid, settings)
  }

  @DeprecatedMethod("ai.h2o.sparkling.ml.models.H2OMOJOModel.createFromMojo")
  def createFromMojo(mojoData: Array[Byte], uid: String): T = createFromMojo(mojoData, uid, H2OMOJOSettings.default)

  def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): T
}
