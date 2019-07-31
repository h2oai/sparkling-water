package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.{DatasetShape, SchemaUtils}
import org.apache.spark.h2o.converters.RowConverter
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{DataFrame, Dataset}

trait H2OMOJOFlattenedInput {
  protected def inputColumnNames: Array[String]

  protected def outputColumnName: String

  protected def applyPredictionUdf(
      dataset: Dataset[_],
      udfConstructor: Array[String] => UserDefinedFunction): DataFrame = {
    val originalDF = dataset.toDF()
    DatasetShape.getDatasetShape(dataset.schema) match {
      case DatasetShape.Flat => applyPredictionUdfToFlatDataFrame(originalDF, udfConstructor, inputColumnNames)
      case DatasetShape.StructsOnly | DatasetShape.Nested =>
        val flattenedDF = SchemaUtils.appendFlattenedStructsToDataFrame(originalDF, RowConverter.temporaryColumnPrefix)
        val inputs = inputColumnNames ++ inputColumnNames.map(s => RowConverter.temporaryColumnPrefix + "." + s)
        val flatWithPredictionsDF = applyPredictionUdfToFlatDataFrame(flattenedDF, udfConstructor, inputs)
        flatWithPredictionsDF.schema.foldLeft(flatWithPredictionsDF) { (df, field) =>
          if (field.name.startsWith(RowConverter.temporaryColumnPrefix)) df.drop(field.name) else df
        }
    }
  }

  private def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = flatDataFrame.columns.intersect(inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)
    flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*)))
  }
}
