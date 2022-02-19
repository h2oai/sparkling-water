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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Goes over RDD partitions counting records and checking if given set of columns has constant values
  */
private[backend] object PartitionStatsGenerator {

  def getPartitionStats(
      rdd: RDD[Row],
      maybeFeatureColumnsForConstantCheck: Option[Seq[String]] = None): PartitionStats = {
    def rowCountWithFeatureColumnConstantCheck(
        partitionIdx: Int,
        iterator: Iterator[Row],
        featureColumns: Seq[String]) = {
      var atMostTwoDistinctFeatureColumnValues = Set[Map[String, Any]]()
      var recordCount = 0
      while (iterator.hasNext) {
        if (atMostTwoDistinctFeatureColumnValues.size < 2) {
          atMostTwoDistinctFeatureColumnValues += iterator.next().getValuesMap(featureColumns)
        } else {
          iterator.next()
        }
        recordCount += 1
      }
      Iterator.single(Map(partitionIdx -> recordCount), atMostTwoDistinctFeatureColumnValues)
    }

    val partitionStats = rdd
      .mapPartitionsWithIndex {
        case (partitionIdx, iterator) =>
          maybeFeatureColumnsForConstantCheck
            .map(rowCountWithFeatureColumnConstantCheck(partitionIdx, iterator, _))
            .getOrElse(Iterator.single(Map(partitionIdx -> iterator.size), Set.empty))
      }
      .fold((Map.empty, Set.empty))((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    val areFeatureColumnsConstant = if (partitionStats._2.isEmpty || maybeFeatureColumnsForConstantCheck.isEmpty) {
      None
    } else {
      Some(partitionStats._2.size < 2)
    }
    PartitionStats(partitionStats._1, areFeatureColumnsConstant)
  }

}
