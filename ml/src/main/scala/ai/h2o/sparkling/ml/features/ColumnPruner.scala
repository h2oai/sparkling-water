package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.params.ColumnPrunerParams
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Column pruner removes specified columns in the input dataset
  */
class ColumnPruner(override val uid: String) extends Transformer with ColumnPrunerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("columnPruner"))

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val columnsToLeft = if (getKeep()) {
      schema.fieldNames.filter(getColumns().contains(_))
    } else {
      schema.fieldNames.filter(!getColumns().contains(_))
    }

    StructType(columnsToLeft.map { col =>
      StructField(col, schema(col).dataType, schema(col).nullable, schema(col).metadata)
    })
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val columnsToRemove = if (getKeep()) {
      dataset.columns.filter(!getColumns().contains(_))
    } else {
      dataset.columns.filter(getColumns().contains(_))
    }
    var resultDataset = dataset
    columnsToRemove.foreach { col =>
      resultDataset = resultDataset.drop(col)
    }
    resultDataset.toDF()
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object ColumnPruner extends H2OParamsReadable[ColumnPruner]
