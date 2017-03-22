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
package org.apache.spark.h2o

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SparkTestContext
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
  * Tests for known Spark issues and our workaround which doesn't fit to any category in other tests
  */
@RunWith(classOf[JUnitRunner])
class KnownSparkIssues extends FunSuite
with Matchers with BeforeAndAfter with SparkTestContext {

  override def beforeAll(){
    super.beforeAll()
    // we use local-cluster since the non-determinism isn't reproducible in local mode
    sc = new SparkContext("local-cluster[2,2,2048]", "test-local-cluster", defaultSparkConf)
    sqlc = SQLContext.getOrCreate(sc)
  }

  test("PUBDEV-3808 - Spark's BroadcastHashJoin is non deterministic - Negative test") {
    val dataFile = getClass.getResource("/PUBDEV-3808_one_nullable_column.parquet").getFile
    val df = sqlc.read.parquet(dataFile).repartition(1).select("id", "strfeat0")

    val sampleA = df.sample(withReplacement = false, 0.1, seed = 0)
    val sampleB = df.sample(withReplacement = false, 0.1, seed = 0)

    val counts = (0 until 5).map( _ => sampleA.except(sampleB).count )
    // The elements shouldn't be the same in this case
    counts.foreach(print(_))
    val first = counts.head
    val mismatch = counts.exists(c => c != first)
    assert(mismatch, "The non-deterministic behaviour should be observable when BroadcastHashJoins are allowed")
  }

  // This test is not reproducible on Spark 1.6. Note - the way how broadcast hash join works has changed in spark 2.0
  // which might be the issue on corresponding spark 2.x branch
  ignore("PUBDEV-3808 - Spark's BroadcastHashJoin is non deterministic - Positive test") {
    val dataFile = getClass.getResource("/PUBDEV-3808_one_nullable_column.parquet").getFile
    val df = sqlc.read.parquet(dataFile).repartition(1).select("id", "strfeat0")

    // disable BroadcastHashJoins
    sqlc.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
    val sampleA = df.sample(withReplacement = false, 0.1, seed = 0)
    val sampleB = df.sample(withReplacement = false, 0.1, seed = 0)

    val counts = (0 until 5).map( _ => sampleA.except(sampleB).count )
    // check whether all elements are the same
    val first = counts.head
    val mismatch = counts.exists(c => c != first)
    assert(!mismatch, "Number of elements in all samples should be the same since BroadcastHashJoins aren't used")
  }
}
