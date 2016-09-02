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

package org.apache.spark.ml.h2o

import _root_.org.apache.spark.sql.Dataset
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Pipeline.SharedReadWrite
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}

/**
  * Exact Spark pipeline with new pipeline stage called OneTimeTransformer. This transformer is called only during the
  * pipeline.fit so can be used to do some additional work during fitting the model. This transformer is removed from
  * list of transformers in the PipelineModel since we don't want to execute this estimator also during prediction
  */
class H2OPipeline(override val uid: String) extends Pipeline {
  def this() = this(Identifiable.randomUID("pipeline"))

  override def fit(dataset: Dataset[_]): PipelineModel = {
    val model = super.fit(dataset)
    val newStages = model.stages.filter(p=> !p.isInstanceOf[OneTimeTransformer])
    new PipelineModel(model.uid,newStages).setParent(model.parent)
  }
}

object H2OPipeline extends MLReadable[H2OPipeline] {

  @Since("1.6.0")
  override def read: MLReader[H2OPipeline] = new H2OPipelineReader

  @Since("1.6.0")
  override def load(path: String): H2OPipeline = super.load(path)

  private class H2OPipelineReader extends MLReader[H2OPipeline] {

    /** Checked against metadata when loading model */
    private val className = classOf[H2OPipeline].getName

    override def load(path: String): H2OPipeline = {
      val (uid: String, stages: Array[PipelineStage]) = SharedReadWrite.load(className, sc, path)
      new H2OPipeline(uid).setStages(stages)
    }
  }
}

/**
  * Special kind of transformer which is executed only in the H2OPipeline.fit call
  */
abstract class OneTimeTransformer extends Transformer {
}

