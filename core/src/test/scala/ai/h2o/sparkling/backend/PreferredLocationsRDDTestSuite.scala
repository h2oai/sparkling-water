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

package ai.h2o.sparkling.backend

import ai.h2o.sparkling.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PreferredLocationsRDDTestSuite extends FunSuite with SparkTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test(s"Test PreferredLocationsRDD is processed correctly even if locations are incorrect") {
    val baseRDD = spark.sparkContext.parallelize(1 to 100, 10)
    val assignments = (0 until 10).map(i => i -> s"executor_8.8.8.8_$i").toMap

    val rddToTest = new PreferredLocationsRDD[Int](assignments, baseRDD.map(_ * 2))
    val result = rddToTest.collect()

    result.sorted shouldEqual (2 to 200 by 2).toArray
  }
}
