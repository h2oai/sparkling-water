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

  def getPartitionStats(rdd: RDD[Row], maybeColumnsForConstantCheck: Option[Seq[String]] = None): PartitionStats = {
    val partitionStats = rdd
      .mapPartitionsWithIndex {
        case (partitionIdx, iterator) =>
          maybeColumnsForConstantCheck
            .map(rowCountWithColumnsConstantCheck(partitionIdx, iterator, _))
            .getOrElse(rowCountWithoutColumnsConstantCheck(partitionIdx, iterator))
      }
      .fold((Map.empty, Set.empty))((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    val areProvidedColumnsConstant = if (partitionStats._2.isEmpty || maybeColumnsForConstantCheck.isEmpty) {
      None
    } else {
      Some(partitionStats._2.size < 2)
    }
    PartitionStats(partitionStats._1, areProvidedColumnsConstant)
  }

  private def rowCountWithoutColumnsConstantCheck(partitionIdx: Int, iterator: Iterator[Row]) =
    Iterator.single(Map(partitionIdx -> iterator.size), Set.empty)

  private def rowCountWithColumnsConstantCheck(
      partitionIdx: Int,
      iterator: Iterator[Row],
      columnsForConstantCheck: Seq[String]) = {
    var atMostTwoDistinctColumnSetValues = Set[Map[String, Any]]()
    var recordCount = 0
    var constantCheckColumnsFlattened: Option[Seq[String]] = None
    while (iterator.hasNext) {
      val row = iterator.next()
      if (constantCheckColumnsFlattened.isEmpty) {
        constantCheckColumnsFlattened = Some(
          findFlattenedColumnNamesByPrefix(columnsForConstantCheck, row.schema.fieldNames))
      }
      if (atMostTwoDistinctColumnSetValues.size < 2) {
        atMostTwoDistinctColumnSetValues += row.getValuesMap(constantCheckColumnsFlattened.get)
      }
      recordCount += 1
    }
    Iterator.single(Map(partitionIdx -> recordCount), atMostTwoDistinctColumnSetValues)
  }

  private def findFlattenedColumnNamesByPrefix(
      columnPrefixes: Seq[String],
      flattenedFields: Array[String]): Seq[String] =
    columnPrefixes.flatMap(
      colNameBeforeFlatten =>
        flattenedFields
          .filter(col => col == colNameBeforeFlatten || col.startsWith(colNameBeforeFlatten + ".")))

}
