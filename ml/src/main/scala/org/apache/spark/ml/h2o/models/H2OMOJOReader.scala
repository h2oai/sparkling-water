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

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.h2o.SparkSpecificUtils
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.util.Utils
import org.json4s.JsonAST

private[models] class H2OMOJOReader[T <: HasMojoData] extends DefaultParamsReader[T] {

  override def load(path: String): T  = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]

    val parsedParams = metadata.params.asInstanceOf[JsonAST.JObject].obj.map(_._1)
    val allowedParams = instance.params.map(_.name)
    val filteredParams = parsedParams.diff(allowedParams)

    SparkSpecificUtils.setTransformerParams(metadata, instance, filteredParams)
    val model = instance.asInstanceOf[T]

    val inputPath = new Path(path, H2OMOJOProps.serializedFileName)
    val fs = inputPath.getFileSystem(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)
    val mojoData = Stream.continually(is.read()).takeWhile(_ != -1).map(_.toByte).toArray
    model.setMojoData(mojoData)
    model
  }

}
