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

import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.types.DataTypes
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2ODeepLearningTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext(
    "local[*]", "test-local",
    conf = defaultSparkConf
  )

  test("Basic DL test") {
    val h2oContext = H2OContext.getOrCreate(sc)
    import spark.implicits._
    val inp = Seq(
      Seq(1987, 10, 14, 3, 741, 730, 912, 849, 1451, 91, 79, 23, 11, 447, 0, 0, 1),
      Seq(1987, 10, 15, 4, 729, 730, 903, 849, 1451, 94, 79, 14, -1, 447, 0, 0, 0),
      Seq(1987, 10, 17, 6, 741, 730, 918, 849, 1451, 97, 79, 29, 11, 447, 0, 0, 1),
      Seq(1987, 10, 18, 7, 729, 730, 847, 849, 1451, 78, 79, -2, -1, 447, 0, 0, 1),
      Seq(1987, 10, 19, 1, 749, 730, 922, 849, 1451, 93, 79, 33, 19, 447, 0, 0, 0),
      Seq(1987, 10, 21, 3, 728, 730, 848, 849, 1451, 80, 79, -1, -2, 447, 0, 0, 1),
      Seq(1987, 10, 22, 4, 728, 730, 852, 849, 1451, 84, 79, 3, -2, 447, 0, 0, 1)
    ).toDF()

    val airlinesData = h2oContext.asH2OFrame(inp)

    airlinesData.replace(airlinesData.numCols() - 1, airlinesData.lastVec().toCategoricalVec)
    airlinesData.update()

    val slModel: H2ODeepLearningModel = new H2ODeepLearning()(hc, sqlContext)
      .setTrainKey(airlinesData.key)
      .setLabelCol("value16")
      .setEpochs(10)
      .fit(null)

    val slPrediction = slModel.transform(inp)

    val index: Int = slPrediction.schema.fieldIndex("predict")
    assert(slPrediction.schema.fields(index).dataType == DataTypes.DoubleType)
    assert(slPrediction.schema.fields.length == 20)

    val slAUC = new BinaryClassificationEvaluator()
      .setLabelCol("value16")
      .setRawPredictionCol("predict")
      .evaluate(slPrediction)
    assert(slAUC <= 1.0 && slAUC >= 0.5)
  }

}
