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

import ai.h2o.sparkling.ml.utils.H2OReaderBase
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.hadoop.fs.Path


private[models] class H2OMOJOReader[T <: HasMojoData] extends H2OReaderBase[T] {

  override def load(path: String): T = {
    val model = super.load(path)

    val inputPath = new Path(path, H2OMOJOProps.serializedFileName)
    val fs = inputPath.getFileSystem(SparkSessionUtils.active.sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)
    val mojoData = Stream.continually(is.read()).takeWhile(_ != -1).map(_.toByte).toArray
    model.setMojoData(mojoData)
    model
  }

}
