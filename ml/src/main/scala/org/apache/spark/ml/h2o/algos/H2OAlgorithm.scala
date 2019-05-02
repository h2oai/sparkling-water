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

import hex.Model
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.h2o.param.{H2OAlgoParams, H2OModelParams}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model => SparkModel}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import water.Key
import water.support.H2OFrameSupport

import scala.reflect.ClassTag
/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
abstract class H2OAlgorithm[P <: Model.Parameters : ClassTag, M <: SparkModel[M] : ClassTag]
  extends Estimator[M] with DefaultParamsWritable with H2OAlgoParams[P] {

  override def fit(dataset: Dataset[_]): M = {
    import org.apache.spark.sql.functions.col

    // Update H2O params based on provided configuration
    updateH2OParams()

    // if this is left empty select
    if (getFeaturesCols().isEmpty) {
      setFeaturesCols(dataset.columns.filter(n => n.compareToIgnoreCase(getLabelCol()) != 0))
    }

    val cols = getFeaturesCols().map(col) ++ Array(col(getLabelCol()))
    val input = H2OContext.getOrCreate(SparkSession.builder().getOrCreate()).asH2OFrame(dataset.select(cols: _*).toDF())

    // check if we need to do any splitting
    if (getTrainRatio() < 1.0) {
      // need to do splitting
      val keys = H2OFrameSupport.split(input, Seq(Key.rand(), Key.rand()), Seq(getTrainRatio()))
      parameters._train = keys(0)._key
      if (keys.length > 1) {
        parameters._valid = keys(1)._key
      }
    } else {
      parameters._train = input._key
    }

    val trainFrame = parameters._train.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }
    H2OFrameSupport.columnsToCategorical(trainFrame, getColumnsToCategorical())

    if ((parameters._distribution == DistributionFamily.bernoulli
      || parameters._distribution == DistributionFamily.multinomial)
      && !trainFrame.vec(getLabelCol()).isCategorical) {
      trainFrame.replace(trainFrame.find(getLabelCol()),
        trainFrame.vec(getLabelCol()).toCategoricalVec).remove()
    }
    water.DKV.put(trainFrame)
    
    // Train
    val model: M with H2OModelParams = trainModel(parameters)

    // pass some parameters set on algo to model
    model.setFeaturesCols(getFeaturesCols())
    model.setLabelCol(getLabelCol())
    model.setConvertUnknownCategoricalLevelsToNa(getConvertUnknownCategoricalLevelsToNa())
    model
  }

  def trainModel(params: P): M with H2OModelParams

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema.fields.exists(f => f.name.compareToIgnoreCase(getLabelCol()) == 0),
      s"Specified label column '${getLabelCol()} was not found in input dataset!")
    require(!getFeaturesCols().exists(n => n.compareToIgnoreCase(getLabelCol()) == 0),
      s"Specified input features cannot contain the label column!")
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def defaultFileName: String
}

object H2OAlgorithmReader {
  def create[A <: H2OAlgorithm[_, _] : ClassTag](defaultFileName: String) = new H2OAlgorithmReader[A](defaultFileName)
}

private[algos] class H2OAlgorithmReader[A <: H2OAlgorithm[_, _] : ClassTag]
(val defaultFileName: String) extends MLReader[A] {

  override def load(path: String): A = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)

    val h2oAlgo = make[A](metadata.uid)
    metadata.getAndSetParams(h2oAlgo)
    h2oAlgo
  }

  private def make[CT: ClassTag](uid: String): CT = {
    val aClass = implicitly[ClassTag[CT]].runtimeClass
    val ctor = aClass.getConstructor(classOf[String])
    ctor.newInstance(uid).asInstanceOf[CT]
  }
}

