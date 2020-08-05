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

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LocalityOptimizerTestSuite extends FunSuite with Matchers {

  test(s"Test a empty list of partitions is reshuffled correctly") {
    val partitionsWithLocations = Seq.empty[(Int, String)]
    val uploadPlan = Map.empty[Int, NodeDesc]
    val expected = Seq.empty[Int]

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test one partition is reshuffled correctly") {
    val partitionsWithLocations = Seq((5, "a"))
    val uploadPlan = Map(1 -> NodeDesc("a", "a", 1))
    val expected = Seq(5)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test a localized list of partitions is reshuffled correctly") {
    val partitionsWithLocations = Seq((5, "a"), (1, "c"), (4, "b"), (9, "c"), (2, "a"), (6, "b"), (8, "b"))
    val uploadPlan = Map(
      1 -> NodeDesc("differentA", "differentA", 1),
      2 -> NodeDesc("differentC", "differentC", 1),
      3 -> NodeDesc("differentB", "differentB", 1),
      4 -> NodeDesc("differentC", "differentC", 1),
      5 -> NodeDesc("differentA", "differentA", 1),
      6 -> NodeDesc("differentB", "differentB", 1),
      7 -> NodeDesc("differentB", "differentB", 1))
    val expected = Seq(5, 1, 4, 9, 2, 6, 8)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test a list with different addresses than the update plan is reshuffled correctly") {
    val partitionsWithLocations = Seq((5, "a"), (1, "c"), (4, "b"), (9, "c"), (2, "a"), (6, "b"), (8, "b"))
    val uploadPlan = Map(
      1 -> NodeDesc("a", "a", 1),
      2 -> NodeDesc("c", "c", 1),
      3 -> NodeDesc("b", "b", 1),
      4 -> NodeDesc("c", "c", 1),
      5 -> NodeDesc("a", "a", 1),
      6 -> NodeDesc("b", "b", 1),
      7 -> NodeDesc("b", "b", 1))
    val expected = Seq(5, 1, 4, 9, 2, 6, 8)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test a mixed list of partitions #1 is reshuffled correctly") {
    val partitionsWithLocations = Seq((5, "a"), (1, "c"), (4, "b"), (9, "c"), (2, "a"), (6, "b"), (8, "b"))
    val uploadPlan = Map(
      1 -> NodeDesc("b", "b", 1),
      2 -> NodeDesc("a", "a", 1),
      3 -> NodeDesc("b", "b", 1),
      4 -> NodeDesc("a", "a", 1),
      5 -> NodeDesc("a", "a", 1),
      6 -> NodeDesc("a", "a", 1),
      7 -> NodeDesc("c", "c", 1))
    val expected = Seq(4, 5, 6, 2, 8, 1, 9)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test a mixed list of partitions #2 is reshuffled correctly") {
    val partitionsWithLocations = Seq((5, "a"), (1, "c"), (4, "b"), (9, "c"), (2, "a"), (6, "b"), (8, "b"))
    val uploadPlan = Map(
      1 -> NodeDesc("c", "c", 1),
      2 -> NodeDesc("a", "a", 1),
      3 -> NodeDesc("b", "b", 1),
      4 -> NodeDesc("b", "b", 1),
      5 -> NodeDesc("a", "a", 1),
      6 -> NodeDesc("a", "a", 1),
      7 -> NodeDesc("c", "c", 1))
    val expected = Seq(1, 5, 4, 6, 2, 8, 9)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test a mixed list of partitions #1 with an unordered update plan is reshuffled correctly") {
    val partitionsWithLocations = Seq((5, "a"), (1, "c"), (4, "b"), (9, "c"), (2, "a"), (6, "b"), (8, "b"))
    val uploadPlan = Map(
      3 -> NodeDesc("b", "b", 1),
      4 -> NodeDesc("a", "a", 1),
      5 -> NodeDesc("a", "a", 1),
      6 -> NodeDesc("a", "a", 1),
      1 -> NodeDesc("b", "b", 1),
      7 -> NodeDesc("c", "c", 1),
      2 -> NodeDesc("a", "a", 1))
    val expected = Seq(4, 5, 6, 2, 8, 1, 9)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }

  test(s"Test a mixed list of partitions with some unknown adresses is reshuffled correctly") {
    val partitionsWithLocations =
      Seq((5, "unknown"), (1, "c"), (4, "unknown"), (9, "unkown"), (2, "a"), (6, "b"), (8, "unknown"))
    val uploadPlan = Map(
      1 -> NodeDesc("b", "b", 1),
      2 -> NodeDesc("a", "a", 1),
      3 -> NodeDesc("b", "b", 1),
      4 -> NodeDesc("a", "a", 1),
      5 -> NodeDesc("a", "a", 1),
      6 -> NodeDesc("a", "a", 1),
      7 -> NodeDesc("c", "c", 1))
    val expected = Seq(6, 2, 4, 9, 8, 5, 1)

    val result = LocalityOptimizer.reshufflePartitions(partitionsWithLocations, uploadPlan)

    result shouldEqual expected
  }
}
