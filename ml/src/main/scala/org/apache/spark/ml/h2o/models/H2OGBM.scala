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

import hex.schemas.GBMV3.GBMParametersV3
import hex.tree.SharedTreeModel.SharedTreeParameters
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader, MLWritable}
import org.apache.spark.sql.SQLContext

/**
  * H2O GBM Algo exposed via Spark ML pipelines.
  */
class H2OGBMModel(model: GBMModel, override val uid: String)(h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OModel[H2OGBMModel,GBMModel](model, h2oContext, sqlContext) with MLWritable {

  def this(model: GBMModel)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(model, Identifiable.randomUID("gbmModel"))(h2oContext, sqlContext)

  override def defaultFileName: String = H2OGBMModel.defaultFileName
}

object H2OGBMModel extends MLReadable[H2OGBMModel] {
  val defaultFileName = "gbm_model"

  @Since("1.6.0")
  override def read: MLReader[H2OGBMModel] = new H2OModelReader[H2OGBMModel, GBMModel](defaultFileName) {
    override protected def make(model: GBMModel, uid: String)
                               (implicit h2oContext: H2OContext,sqLContext: SQLContext): H2OGBMModel = new H2OGBMModel(model, uid)(h2oContext, sqlContext)
  }

  @Since("1.6.0")
  override def load(path: String): H2OGBMModel = super.load(path)
}

class H2OGBM(parameters: Option[GBMParameters], override val uid: String)
                     (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OAlgorithm[GBMParameters, H2OGBMModel](parameters)
          with H2OGBMParams {

  type SELF = H2OGBM

  override def defaultFileName: String = H2OGBM.defaultFileName

  override def trainModel(params: GBMParameters): H2OGBMModel = {
    val model = new GBM(params).trainModel().get()
    new H2OGBMModel(model)
  }

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("dl"))
  def this(parameters: GBMParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters),Identifiable.randomUID("dl"))
  def this(parameters: GBMParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters),uid)
}

object H2OGBM extends MLReadable[H2OGBM] {

  private final val defaultFileName = "gbm_params"

  @Since("1.6.0")
  override def read: MLReader[H2OGBM] = new H2OAlgorithmReader[H2OGBM, GBMParameters](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OGBM = super.load(path)
}


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OGBMParams extends H2OTreeParams[GBMParameters] {

  type H2O_SCHEMA = GBMParametersV3

  protected def paramTag = reflect.classTag[GBMParameters]
  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  /**
    * All parameters should be set here along with their documentation and explained default values
    */
  final val learnRate = doubleParam("learnRate")
  final val learnRateAnnealing = doubleParam("learnRateAnnealing")
  final val colSampleRate = doubleParam("colSampleRate")
  final val maxAbsLeafnodePred = doubleParam("maxAbsLeafnodePred")
  final val responseColumn = param[String]("responseColumn")

  setDefault(learnRate -> parameters._learn_rate)
  setDefault(learnRateAnnealing -> parameters._learn_rate_annealing)
  setDefault(colSampleRate -> parameters._col_sample_rate)
  setDefault(maxAbsLeafnodePred -> parameters._max_abs_leafnode_pred)
  setDefault(responseColumn -> parameters._response_column)
}

trait H2OTreeParams[P <: SharedTreeParameters] extends H2OParams[P] {
  final val ntrees = intParam("ntrees")
  final val maxDepth = intParam("maxDepth")
  final val minRows = doubleParam("minRows")
  final val nbins = intParam("nbins")
  final val nbinsCat = intParam("nbinsCats")
  final val minSplitImprovement = doubleParam("minSplitImprovement")
  final val r2Stopping = doubleParam("r2Stopping")
  final val seed = longParam("seed")
  final val nbinsTopLevel = intParam("nbinsTopLevel")
  final val buildTreeOneNode = booleanParam("buildTreeOneNode")
  final val scoreTreeInterval = intParam("scoreTreeInterval")
  final val sampleRate = doubleParam("sampleRate")

  setDefault(
    ntrees -> parameters._ntrees,
    maxDepth -> parameters._max_depth,
    minRows -> parameters._min_rows,
    nbins -> parameters._nbins,
    nbinsCat -> parameters._nbins_cats,
    minSplitImprovement -> parameters._min_split_improvement,
    r2Stopping -> parameters._r2_stopping,
    seed -> parameters._seed,
    nbinsTopLevel -> parameters._nbins_top_level,
    buildTreeOneNode -> parameters._build_tree_one_node,
    scoreTreeInterval -> parameters._score_tree_interval,
    sampleRate -> parameters._sample_rate)

}
