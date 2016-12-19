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
package org.apache.spark.ml.spark.models.svm

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.mllib.classification
import org.apache.spark.mllib.linalg.Vectors
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.support.H2OFrameSupport

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SVMModelTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Should score the same regression value.") {
    import sqlContext.implicits._
    val h2oContext = hc
    import h2oContext.implicits._

    // Generate random training data
    val trainRDD = sc.parallelize(1 to 50, 1).map(v => {
      val values = Array.fill(5){0}.map(x => Random.nextDouble())
      //val label = Math.round(Random.nextDouble())
      val label = if (Math.round(Random.nextDouble()) > 0.5) "1" else "0"
      (label, Vectors.dense(values))
    }).cache()
    val trainDF = trainRDD.toDF("Label", "Vector")

    val trainFrame = h2oContext.asH2OFrame(trainDF, "bubbles")
    H2OFrameSupport.withLockAndUpdate(trainFrame){ fr =>
      fr.replace(0, fr.vec(0).toCategoricalVec).remove()
    }
    val initialWeights = Vectors.dense(1, 1, 1, 1, 1)
    val weightsDF = sc.parallelize(Array(Tuple1(initialWeights))).toDF("Vector")
    val weightsFrame = hc.asH2OFrame(weightsDF, "weights")

    // Learning parameters
    val parms = new SVMParameters
    parms._train = trainFrame
    parms._response_column = 'Label
    parms._initial_weights = weightsFrame

    val svm = new SVM(parms, h2oContext)

    // Train model
    val h2oSVMModel: SVMModel = svm.trainModel.get

    val sparkSVMModel = new classification.SVMModel(
      Vectors.dense(h2oSVMModel.output.weights),
      h2oSVMModel.output.interceptor
    )

    // Make sure both scoring methods return the same results
    val h2oPredsVec = h2oSVMModel.score(trainFrame).vec(0)
    val h2oPreds = (0 until 50).map(h2oPredsVec.at(_)).toArray
    val sparkPreds = sparkSVMModel.predict(trainRDD.map(_._2)).collect()

    assert( h2oPreds.sameElements(sparkPreds) )
  }
}
