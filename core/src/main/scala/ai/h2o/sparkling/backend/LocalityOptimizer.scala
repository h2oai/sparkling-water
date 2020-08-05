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

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.TaskContext
import org.apache.spark.expose.Logging
import org.apache.spark.sql.Row

private[backend] object LocalityOptimizer extends Logging {
  private type UploadPlan = Map[Int, NodeDesc]

  def reshufflePartitions(nonEmptyPartitions: Seq[Int], uploadPlan: UploadPlan, rdd: H2OAwareRDD[Row]): Seq[Int] = {
    require(nonEmptyPartitions.size == uploadPlan.size)
    type SparkJob = (TaskContext, Iterator[Row]) => (Int, String)
    val currentLocationsJob: SparkJob = getPartitionLocations(nonEmptyPartitions)
    val currentLocations = SparkSessionUtils.active.sparkContext.runJob(rdd, currentLocationsJob, nonEmptyPartitions)

    val partitionsWithLocations = currentLocations.sortBy(_._1).map(l => (nonEmptyPartitions(l._1), l._2))
    logDebug(s"Estimated current partition locations for RDD ${rdd.name}: ${partitionsWithLocations}")

    reshufflePartitions(partitionsWithLocations, uploadPlan)
  }

  def reshufflePartitions(partitionsWithLocations: Seq[(Int, String)], uploadPlan: UploadPlan): Seq[Int] = {
    require(partitionsWithLocations.size == uploadPlan.size)
    val sourceLocations = partitionsWithLocations.toArray
    val destinationLocations = uploadPlan.toArray.sortBy(_._1).map(_._2.hostname)
    var pairToSwapOption = getPairToSwap(sourceLocations, destinationLocations, 0)
    while (pairToSwapOption.isDefined) {
      val pairToSwap = pairToSwapOption.get
      val temp = sourceLocations(pairToSwap._1)
      sourceLocations(pairToSwap._1) = sourceLocations(pairToSwap._2)
      sourceLocations(pairToSwap._2) = temp
      pairToSwapOption = getPairToSwap(sourceLocations, destinationLocations, pairToSwap._1)
    }

    sourceLocations.map(_._1)
  }

  private def getPairToSwap(
      sourceLocations: Array[(Int, String)],
      destinationLocations: Array[String],
      startAtIndex: Int): Option[(Int, Int)] = {
    var destinationIndex = startAtIndex
    var result: Option[(Int, Int)] = None
    while (result.isEmpty && destinationIndex < destinationLocations.length) {
      if (sourceLocations(destinationIndex)._2 == destinationLocations(destinationIndex)) {
        destinationIndex += 1
      }
      var sourceIndex = startAtIndex + 1
      while (result.isEmpty && sourceIndex < sourceLocations.length) {
        if (destinationLocations(destinationIndex) == sourceLocations(sourceIndex)._2) {
          result = Some((destinationIndex, sourceIndex))
        }
        sourceIndex += 1
      }
      destinationIndex += 1
    }
    result
  }

  private def getPartitionLocations(partitions: Seq[Int])(context: TaskContext, it: Iterator[Row]): (Int, String) = {
    val chunkIdx = partitions.indexOf(context.partitionId())
    val address = java.net.InetAddress.getLocalHost().getHostAddress
    (chunkIdx, address)
  }
}
