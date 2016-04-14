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
package water.app

import java.io.{File, FileOutputStream, InputStream, OutputStream}
import java.net.URI

import hex.Distribution.Family
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.tree.gbm.GBMModel
import hex.{Model, ModelMetrics}
import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import water.fvec.Frame
import water.persist.Persist
import water.{AutoBuffer, H2O, Key, Keyed}

/**
 * A simple application trait to define Sparkling Water applications.
 */
trait SparklingWaterApp {

  @transient val sc: SparkContext
  @transient val sqlContext: SQLContext
  @transient val h2oContext: H2OContext

  def loadH2OFrame(datafile: String) = new H2OFrame(new java.net.URI(datafile))

  def shutdown(): Unit = {
    // Shutdown Spark
    sc.stop()
    // Shutdown H2O explicitly (at least the driver)
    h2oContext.stop()
  }
}

/**
  * Interface to access different model metrics provided by H2O model.
  */
trait ModelMetricsSupport {

  /* Helper class to have nice API */
  class ModelMetricsExtractor[T <: ModelMetrics] {
    def apply[M <: Model[M, P, O], P <: hex.Model.Parameters, O <: hex.Model.Output]
             (model: Model[M,P,O], fr: Frame): T = {
      // Fetch model metrics and rescore if it is necessary
      if (ModelMetrics.getFromDKV(model, fr).asInstanceOf[T] == null) {
        model.score(fr).delete()
      }
      ModelMetrics.getFromDKV(model, fr).asInstanceOf[T]
    }
  }

  def modelMetrics[T <: ModelMetrics] = new ModelMetricsExtractor[T]
}

// Create companion object
object ModelMetricsSupport extends ModelMetricsSupport

trait DeepLearningSupport {

  def DLModel(train: H2OFrame, valid: H2OFrame, response: String,
              modelId: String = "model",
              epochs: Int = 10, l1: Double = 0.0001, l2: Double = 0.0001,
              activation: Activation = Activation.RectifierWithDropout,
              hidden: Array[Int] = Array(200, 200)): DeepLearningModel = {

    val dlParams = new DeepLearningParameters()
    dlParams._train = train._key
    dlParams._valid = if (valid != null) {
      valid._key
    } else {
      null
    }
    dlParams._response_column = response
    dlParams._epochs = epochs
    dlParams._l1 = l1
    dlParams._l2 = l2
    dlParams._activation = activation
    dlParams._hidden = hidden

    // Create a job
    val dl = new DeepLearning(dlParams, water.Key.make(modelId))
    val model = dl.trainModel.get
    model
  }
}

// Create companion object
object DeepLearningSupport extends DeepLearningSupport

trait GBMSupport {

  def GBMModel(train: H2OFrame, test: H2OFrame, response: String,
               modelId: String = "model",
               ntrees: Int = 50, depth: Int = 6, family: Family = Family.AUTO): GBMModel = {
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = train._key
    gbmParams._valid = if (test != null) {
      test._key
    } else {
      null
    }
    gbmParams._response_column = response
    gbmParams._ntrees = ntrees
    gbmParams._max_depth = depth
    gbmParams._distribution = family

    val gbm = new GBM(gbmParams, water.Key.make(modelId))
    val model = gbm.trainModel.get
    model
  }
}


// Create companion object
object GBMSupport extends GBMSupport


trait ModelSerializationSupport {

  def exportH2OModel(model : Model[_,_,_], destination: URI): URI = {
    val modelKey = model._key.asInstanceOf[Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]]]
    val p: Persist = H2O.getPM.getPersistForURI(destination)
    val os: OutputStream = p.create(destination.toString, true)
    model.writeAll(new AutoBuffer(os, true)).close

    destination
  }

  def loadH2OModel[M <: Model[_, _, _]](source: URI) : M = {
    val p: Persist = H2O.getPM.getPersistForURI(source)
    val is: InputStream = p.open(source.toString)
    Keyed.readAll(new AutoBuffer(is)).asInstanceOf[M]
  }

  def exportPOJOModel(model : Model[_, _,_], destination: URI): URI = {
    val destFile = new File(destination)
    val fos = new FileOutputStream(destFile)
    val writer = new model.JavaModelStreamWriter(false)
    try {
      writer.writeTo(fos)
    } finally {
      fos.close()
    }
    destination
  }
}

object ModelSerializationSupport extends ModelSerializationSupport
