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
