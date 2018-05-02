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
package py_sparkling.ml.algos

import ai.h2o.automl.{AutoML, AutoMLBuildSpec}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos.H2OAutoMLReader
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import py_sparkling.ml.models.H2OMOJOModel
import water.support.ModelSerializationSupport

/**
  * H2O AutoML Wrapper for PySparkling
  */
class H2OAutoML(override val automlBuildSpec: Option[AutoMLBuildSpec], override val uid: String)
               (implicit hc: H2OContext, sqlContext: SQLContext)
  extends org.apache.spark.ml.h2o.algos.H2OAutoML {

  def this()(implicit hc: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("automl"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  override def trainModel(aml: AutoML) = {
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(aml.leader()))
  }
}


object H2OAutoML extends MLReadable[H2OAutoML] {

  private final val defaultFileName = "automl_params"

  override def read: MLReader[H2OAutoML] = H2OAutoMLReader.create[H2OAutoML](defaultFileName)

  override def load(path: String): H2OAutoML = super.load(path)
}
