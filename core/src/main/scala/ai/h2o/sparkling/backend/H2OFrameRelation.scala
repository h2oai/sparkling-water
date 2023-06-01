package ai.h2o.sparkling.backend

import ai.h2o.sparkling.backend.utils.ReflectionUtils
import ai.h2o.sparkling.{H2OColumn, H2OColumnType, H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * H2O relation implementing column filter operation.
  */
case class H2OFrameRelation(frame: H2OFrame, copyMetadata: Boolean)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan
  with PrunedScan {

  private lazy val hc = H2OContext.ensure(
    "H2OContext has to be started in order to do " +
      "transformations between Spark and H2O frames.")

  // Get rid of annoying print
  override def toString: String = getClass.getSimpleName

  override val needConversion = false

  override val schema: StructType = createSchema(frame, copyMetadata)

  override def buildScan(): RDD[Row] = new H2ODataFrame(frame)(hc).asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new H2ODataFrame(frame, requiredColumns)(hc).asInstanceOf[RDD[Row]]

  private def extractMetadata(column: H2OColumn, numberOfRows: Long): Metadata = {
    val builder = new MetadataBuilder()
      .putLong("count", numberOfRows)
      .putLong("naCnt", column.numberOfMissingElements)

    if (column.dataType == H2OColumnType.`enum`) {
      builder
        .putLong("cardinality", column.domainCardinality)
      if (column.domain != null) {
        builder.putStringArray("vals", column.domain)
      }
    } else if (Seq(H2OColumnType.int, H2OColumnType.real).contains(column.dataType)) {
      builder
        .putDouble("min", column.min)
        .putDouble("mean", column.mean)
        .putDouble("max", column.max)
        .putDouble("std", column.sigma)
        .putDouble("sparsity", column.numberOfZeros / numberOfRows.toDouble)
    }
    builder.build()
  }

  private def createSchema(f: H2OFrame, copyMetadata: Boolean): StructType = {
    import ReflectionUtils._

    val types = f.columns.map { column =>
      val metadata = if (copyMetadata) extractMetadata(column, f.numberOfRows) else Metadata.empty
      StructField(column.name, dataTypeFor(column), column.nullable, metadata)
    }
    StructType(types)
  }
}
