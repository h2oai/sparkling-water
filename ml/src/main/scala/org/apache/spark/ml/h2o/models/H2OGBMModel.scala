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

import hex.genmodel.MojoModel
import org.apache.spark.annotation.Since
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader, MLWritable}
import org.apache.spark.sql.SQLContext

class H2OGBMModel(model: MojoModel, mojoData: Array[Byte], override val uid: String)(sqlContext: SQLContext)
  extends H2OMOJOModel[H2OGBMModel](model, mojoData, sqlContext) with MLWritable {

  def this(model: MojoModel, mojoData: Array[Byte])(sqlContext: SQLContext) = this(model, mojoData, Identifiable.randomUID("gbmModel"))(sqlContext)

  override def defaultFileName: String = H2OGBMModel.defaultFileName
}

object H2OGBMModel extends MLReadable[H2OGBMModel] {
  val defaultFileName = "gbm_model"

  @Since("1.6.0")
  override def read: MLReader[H2OGBMModel] = new H2OMOJOModelReader[H2OGBMModel](defaultFileName) {
    override protected def make(model: MojoModel, mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): H2OGBMModel = {
      new H2OGBMModel(model, mojoData, uid)(sqlContext)
    }
  }

  @Since("1.6.0")
  override def load(path: String): H2OGBMModel = super.load(path)
}

