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

import hex.glm.GLM
import hex.glm.GLMModel.GLMParameters
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos.{H2OAlgorithmReader, H2OGLMParams}
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import py_sparkling.ml.models.H2OMOJOModel
import water.support.ModelSerializationSupport

/**
  * H2O GLM Wrapper for PySparkling
  */
class H2OGLM(parameters: Option[GLMParameters], override val uid: String)
            (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends org.apache.spark.ml.h2o.algos.H2OGLM(parameters, uid)(h2oContext, sqlContext)
    with H2OGLMParams {

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("glm"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  def this(parameters: GLMParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("glm"))

  def this(parameters: GLMParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)

  override def trainModel(params: GLMParameters): H2OMOJOModel = {
    val model = new GLM(params).trainModel().get()
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(model))
  }
}

object H2OGLM extends MLReadable[H2OGLM] {

  private final val defaultFileName = "glm_params"

  override def read: MLReader[H2OGLM] = H2OAlgorithmReader.create[H2OGLM, GLMParameters](defaultFileName)

  override def load(path: String): H2OGLM = super.load(path)
}
