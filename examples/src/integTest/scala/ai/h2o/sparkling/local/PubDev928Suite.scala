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

package ai.h2o.sparkling.local

import java.io.File

import ai.h2o.sparkling.LocalIntegrationTest
import ai.h2o.sparkling.examples.{Airlines, TestUtils}
import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.fvec.H2OFrame
import water.support.{H2OFrameSupport, SparkContextSupport}


/**
 * PUBDEV-928 test suite.
 *
 * Verifies that DL can be run on 0-length chunks.
 */
@RunWith(classOf[JUnitRunner])
class PubDev928Suite extends LocalIntegrationTest {

  test("Verify scoring on 0-length chunks") {
    launch(PubDev928Test)
  }
}

object PubDev928Test extends SparkContextSupport {

  def main(args: Array[String]): Unit = {
    val conf = configure("PUBDEV-928")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate()
    implicit val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    import sqlContext.implicits._

    val airlinesData = new H2OFrame(new File(TestUtils.locate("smalldata/airlines/allyears2k_headers.zip")))

    // We need to explicitly repartition data to 12 chunks/partitions since H2O parser handling
    // partitioning dynamically based on available number of CPUs
    println("Number of chunks before query: " + airlinesData.anyVec().nChunks())
    val airlinesTable: RDD[Airlines] = h2oContext.asRDD[Airlines](airlinesData)
    airlinesTable.toDF.createOrReplaceTempView("airlinesTable")

    val query = "SELECT * FROM airlinesTable WHERE Dest LIKE 'SFO'"
    // Transform result of SQL query directly into H2OFrame, but change number of
    val queryResult = sqlContext.sql(query)
    val partitionNumbers = queryResult.count().asInstanceOf[Int] + 1
    val result: H2OFrame = h2oContext.asH2OFrame(queryResult.repartition(partitionNumbers), "flightTable")
    println("Number of partitions in query result: " + queryResult.rdd.partitions.length)
    println("Number of chunks in query result" + result.anyVec().nChunks())

    val train: H2OFrame = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed)
    H2OFrameSupport.withLockAndUpdate(train) { fr =>
      fr.replace(fr.numCols() - 1, fr.lastVec().toCategoricalVec)
    }
    println(s"Any vec chunk cnt: ${train.anyVec().nChunks()}")
    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = train.key
    dlParams._response_column = "IsDepDelayed"

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    // THIS WILL FAIL
    val testFrame: H2OFrame = result
    // Verify that testFrame has at least one chunk with 0-rows
    val av = testFrame.anyVec()
    println(s"Test frame chunk cnt: ${av.nChunks()}")
    for (i <- 0 until av.nChunks()) {
      println(av.chunkForChunkIdx(i).len())
    }

    // And run scoring on dataset which contains at least one chunk with zero-lines
    import h2oContext.implicits._
    val predictionH2OFrame = dlModel.score(testFrame)('predict)
    assert(predictionH2OFrame.numRows() == testFrame.numRows())

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}
