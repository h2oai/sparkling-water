package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.{DatasetShape, SchemaUtils}
import org.apache.spark.ExposeUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.{ArrayType, BinaryType, MapType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

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

  protected def getRelevantColumnNames(flatDataFrame: DataFrame, inputs: Array[String]): Array[String] = {
    val result = ArrayBuffer[String]()

    def addIfPrefixExists(prefix: String): Unit = {
      if (inputs.exists(
            input => input.startsWith(prefix + ".") && Try(input.substring(prefix.length + 1).toInt).isSuccess)) {
        result.append(prefix)
      }
    }

    flatDataFrame.schema.fields.foreach { field =>
      field.dataType match {
        case _ if inputs.contains(field.name) => result.append(field.name)
        case _: BinaryType => addIfPrefixExists(field.name)
        case _: ArrayType => addIfPrefixExists(field.name)
        case _: MapType => addIfPrefixExists(field.name)
        case v if ExposeUtils.isAnyVectorUDT(v) => addIfPrefixExists(field.name)
        case _ =>
      }
    }
    result.toArray
  }

  protected def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = getRelevantColumnNames(flatDataFrame, inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)
    flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*)))
  }
}
