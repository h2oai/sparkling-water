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
package ai.h2o.sparkling.backend.api.rdds

import ai.h2o.sparkling._
import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RDDsServletTestSuite extends FunSuite with SharedH2OTestContext with RDDsRestApi {

  override def createSparkSession(): SparkSession =
    sparkSession("local[*]", defaultSparkConf.set("spark.ext.h2o.context.path", "context"))

  test("RDDsHandler.list() method") {
    val rname = "Test"
    val rpart = 21
    val rdd = sc.parallelize(1 to 10, rpart).setName(rname).cache()
    rdd.count()
    val result = listRDDs()
    assert(result.rdds.length == 1, "Number of created and persisted RDDs should be 1")
    assert(result.rdds(0).name.equals(rname), "Name matches")
    assert(result.rdds(0).partitions == rpart, "Number of partitions matches")
  }

  test("RDDsHandler.getRDD() method") {
    val rname = "Test"
    val rpart = 21
    val rdd = sc.parallelize(1 to 100, rpart).setName(rname).cache()

    val result = getRDD(rdd.id)
    assert(result.rdd_id == rdd.id, "Original ID and obtained ID should match")
    assert(result.name.equals(rname), "Name matches")
    assert(result.partitions == rpart, "Number of partitions matches")
  }

  test("RDDsHandler.toH2OFrame() method simple type") {
    val rname = "Test"
    val rpart = 21
    val rdd = sc.parallelize(1 to 100, rpart).setName(rname).cache()

    val result = convertToH2OFrame(rdd.id, "frame_a")
    val h2oframe = H2OFrame(result.h2oframe_id)
    assert(h2oframe.frameId == "frame_a", "H2OFrame ID should be equal to \"requested_name\"")
    assert(h2oframe.numberOfColumns == 1, "Number of columns should match")
    assert(h2oframe.columnNames.sameElements(Seq("value")), "Column names should match")
    assert(h2oframe.numberOfRows == rdd.count(), "Number of rows should match")
  }

  test("RDDsHandler.toH2OFrame() method - product class") {
    val rname = "Test"
    val rpart = 21
    val rdd = sc.parallelize(Seq(A(1, "A"), A(2, "B"), A(3, "C")), rpart).setName(rname).cache()

    val result = convertToH2OFrame(rdd.id, "frame_b")
    val h2oframe = H2OFrame(result.h2oframe_id)
    assert(h2oframe.frameId == "frame_b", "H2OFrame ID should be equal to \"requested_name\"")
    assert(h2oframe.numberOfColumns == 2, "Number of columns should match")
    assert(h2oframe.columnNames.sorted.sameElements(Seq("num", "str")), "Column names should match")
    assert(h2oframe.numberOfRows == rdd.count(), "Number of rows should match")
  }

  test("RDDsHandler.getRDD() method, querying non-existing RDD") {
    intercept[RestApiCommunicationException] {
      val nonExistentRddId = 777
      getRDD(nonExistentRddId)
    }
  }
}

case class A(num: Int, str: String) extends Serializable
