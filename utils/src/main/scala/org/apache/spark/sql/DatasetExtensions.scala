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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.Metadata

object DatasetExtensions {

  implicit class DatasetWrapper[T](dataset: Dataset[T]) {
    def withColumns(colNames: Seq[String], cols: Seq[Column]): DataFrame = {
      colNames.zip(cols).foldLeft(dataset.toDF()) {
        case (currentDataFrame, (columnName, column)) => currentDataFrame.withColumn(columnName, column)
      }
    }

    def withColumn(colName: String, col: Column, metadata: Metadata): DataFrame = {
      dataset.withColumn(colName, col, metadata)
    }

    def h2oLocalCheckpoint(): Dataset[T] = h2oLocalCheckpoint(true)

    def h2oLocalCheckpoint(eager: Boolean): Dataset[T] = {
      val internalRdd = dataset.queryExecution.toRdd.map(_.copy())
      internalRdd.localCheckpoint()

      if (eager) {
        internalRdd.count()
      }

      val physicalPlan = dataset.queryExecution.executedPlan

      def firstLeafPartitioning(partitioning: Partitioning): Partitioning = {
        partitioning match {
          case p: PartitioningCollection => firstLeafPartitioning(p.partitionings.head)
          case p => p
        }
      }

      val outputPartitioning = firstLeafPartitioning(physicalPlan.outputPartitioning)

      implicit val encoder = dataset.exprEnc

      Dataset.ofRows(
        dataset.sparkSession,
        LogicalRDD(
          dataset.logicalPlan.output,
          internalRdd,
          outputPartitioning,
          physicalPlan.outputOrdering
        )(dataset.sparkSession)).as[T]
    }
  }

}
