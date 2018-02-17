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

package org.apache.spark.ml.h2o.algos

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.kmeans.KMeans
import hex.kmeans.KMeansModel.KMeansParameters
import hex.schemas.KMeansV3
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param.{H2OAlgoParams, H2OModelParams}
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.sql.SQLContext
import water.support.ModelSerializationSupport

/*
  H2O KMeans algorithm exposed via Spark ML Pipelines
 */
class H2OKMeans(parameters: Option[KMeansParameters], override val uid: String)
               (implicit h2oContext: H2OContext, sqlContext: SQLContext)
                extends H2OAlgorithm[KMeansParameters, H2OMOJOModel](parameters)
                with H2OKMeansParams {

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("kmeans"))

  def this(uid: String, h2oContext: H2OContext, sqlContext: SQLContext) = this(None, uid)(h2oContext, sqlContext)

  def this(parameters: KMeansParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), Identifiable.randomUID("kmeans"))

  def this(parameters: KMeansParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters), uid)

  override def trainModel(params: KMeansParameters): H2OMOJOModel with H2OModelParams = {
    val model = new KMeans(params).trainModel().get()
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(model))
  }

  override def defaultFileName: String = H2OKMeans.defaultFileName
}

object H2OKMeans extends MLReadable[H2OKMeans] {

  private final val defaultFileName: String = "kmeans_params"

  override def read: MLReader[H2OKMeans] = new H2OAlgorithmReader[H2OKMeans, KMeansParameters](defaultFileName)

  override def load(path: String): H2OKMeans = super.load(path)
}

trait H2OKMeansParams extends H2OAlgoParams[KMeansParameters] {

  type H2O_SCHEMA = KMeansV3

  protected def paramTag = reflect.classTag[KMeansParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]


}
