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
package org.apache.spark.ml.h2o.features

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnVectorizerTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext(
    "local[*]", "test-local",
    conf = defaultSparkConf
  )

  test("Should vectorize features") {
    val h2oContext = H2OContext.getOrCreate(sc)
    import spark.implicits._
    val inp = sc.parallelize(Array(
      // TODO test for strings
      Seq(1,2,3)
    )).toDF()

    val cv = new ColumnVectorizer()
      .setKeep(true)
      .setColumns(Array("test", "test2"))
      .setOutputCol("features")
//    cv.transform(inp).collect().foreach(println)

  }

}
