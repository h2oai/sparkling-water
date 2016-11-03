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
package org.apache.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.ml.spark.models.MissingValuesHandling
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec

@RunWith(classOf[JUnitRunner])
class FrameMLUtilsTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Should compute the running average.") {
    val trainRDD = sc.parallelize(Array(Row(1, 2, 3, 2.0, 1), Row(2, 3, 4, 2.5, 2), Row(3, 4, 5, 3.0, null)))

    val means = FrameMLUtils.movingAverage(
      trainRDD,
      Array(
        StructField("F1", DataTypes.IntegerType),
        StructField("F2", DataTypes.IntegerType),
        StructField("F3", DataTypes.IntegerType),
        StructField("F4", DataTypes.DoubleType),
        StructField("F4", DataTypes.IntegerType)
      ),
      new Array(5)
    )

    assertResult(2)(means(0))
    assertResult(3)(means(1))
    assertResult(4)(means(2))
    assertResult(2.5)(means(3))
    assertResult(1.5)(means(4))

  }

  test("Should properly cast to double") {
    assertResult(15.0)(FrameMLUtils.toDouble(15.asInstanceOf[Byte], StructField("F1", DataTypes.ByteType), Array()))
    assertResult(15.0)(FrameMLUtils.toDouble(15.asInstanceOf[Short], StructField("F1", DataTypes.ShortType), Array()))
    assertResult(15.0)(FrameMLUtils.toDouble(15.asInstanceOf[Integer], StructField("F1", DataTypes.IntegerType), Array()))
    assertResult(15.0)(FrameMLUtils.toDouble(15.asInstanceOf[Double], StructField("F1", DataTypes.DoubleType), Array()))
    assertResult(2.0)(FrameMLUtils.toDouble("15", StructField("F1", DataTypes.StringType), Array("5", "10", "15")))
    val errField: StructField = StructField("F1", DataTypes.BooleanType)
    val errMsg = intercept[IllegalArgumentException] {
      FrameMLUtils.toDouble(true, errField, Array("5", "10", "15"))
    }
    assertResult(s"Target column has to be an enum or a number. ${errField.toString}")(errMsg.getMessage)
  }

  test("Should skip rows with missing values") {
    val h2oFrame = prepareFrameWithNAs

    val labelPoints = FrameMLUtils.toLabeledPoints(
      h2oFrame,
      "C3",
      3,
      MissingValuesHandling.Skip,
      hc,
      sqlContext
    )._1.collect()

    assertResult(17)(labelPoints.length)
    labelPoints.foreach(entry => assertResult(3)(entry.features.numActives))

    //     Clean up
    h2oFrame.delete()
  }

  test("Should imput missing values with averages") {
    val h2oFrame = prepareFrameWithNAs

    val labelPoints = FrameMLUtils.toLabeledPoints(
      h2oFrame,
      "C3",
      3,
      MissingValuesHandling.MeanImputation,
      hc,
      sqlContext
    )._1.collect()

    assertResult(50)(labelPoints.length)
    labelPoints.foreach(entry => {
      assertResult(3)(entry.features.numActives)
      if(entry.label % 3 == 0) {
        assertResult(25.5)(entry.features(0))
      } else if(entry.label % 3 == 1) {
        assertResult(25.757575757575758)(entry.features(1))
        assertResult(0.48484848484848486)(entry.features(2))
      } else {
        assertResult(entry.label)(entry.features(0))
        assertResult(entry.label)(entry.features(1))
      }
    })

    //     Clean up
    h2oFrame.delete()
  }

  private def prepareFrameWithNAs = {
    val fname: String = "testMetadata.hex"
    val colNames: Array[String] = Array("C0", "C1", "C2", "C3")
    val chunkLayout: Array[Long] = Array(50L)
    val data = Array(
      (1L to 50L).map(i => Array[Any](i, i, 0, i)).map(arr => {
        val i = arr(0).asInstanceOf[Long]
        if (i % 3 == 0) Array[Any](null, i, 1, i)
        else if (i % 3 == 1) Array[Any](i, null, null, i)
        else arr
      }).toArray
    )
    makeH2OFrame2(fname, colNames, chunkLayout, data, Array(Vec.T_NUM, Vec.T_NUM, Vec.T_CAT, Vec.T_NUM), Array(null, null, Array("NO", "YES"), null))
  }
}
