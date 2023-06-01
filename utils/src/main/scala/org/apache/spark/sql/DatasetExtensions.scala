package org.apache.spark.sql

import org.apache.spark.sql.types.Metadata

object DatasetExtensions {

  implicit class DatasetWrapper(dataset: Dataset[_]) {
    def withColumns(colNames: Seq[String], cols: Seq[Column]): DataFrame = {
      colNames.zip(cols).foldLeft(dataset.toDF()) {
        case (currentDataFrame, (columnName, column)) => currentDataFrame.withColumn(columnName, column)
      }
    }

    def withColumn(colName: String, col: Column, metadata: Metadata): DataFrame = {
      dataset.withColumn(colName, col, metadata)
    }
  }

}
