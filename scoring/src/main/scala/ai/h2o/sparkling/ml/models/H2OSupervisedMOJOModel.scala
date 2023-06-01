package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OSupervisedMOJOParams
import hex.ModelCategory
import hex.genmodel.MojoModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{DoubleType, StructType}

class H2OSupervisedMOJOModel(override val uid: String) extends H2OAlgorithmMOJOModel(uid) with H2OSupervisedMOJOParams {

  override private[sparkling] def setSpecificParams(mojoModel: MojoModel): Unit = {
    super.setSpecificParams(mojoModel)
    set(offsetCol -> mojoModel._offsetColumn)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val offsetColumn = getOffsetCol()
    if (offsetColumn != null) {
      require(schema.fieldNames.contains(offsetColumn), "Offset column must be present within the dataset!")
    }
    super.transformSchema(schema)
  }

  protected override def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = getRelevantColumnNames(flatDataFrame, inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)

    unwrapMojoModel().getModelCategory match {
      case ModelCategory.Binomial | ModelCategory.Regression | ModelCategory.Multinomial | ModelCategory.Ordinal =>
        val offsetColumn = getOffsetCol()
        if (offsetColumn != null) {
          if (!flatDataFrame.columns.contains(offsetColumn)) {
            throw new RuntimeException("Offset column must be present within the dataset!")
          }
          flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*), col(getOffsetCol()).cast(DoubleType)))
        } else {
          // Methods of EasyPredictModelWrapper for given prediction categories take offset as parameter.
          // `lit(0.0)` represents a column with zero values (offset disabled).
          flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*), lit(0.0)))
        }
      case _ =>
        flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*)))
    }
  }
}

object H2OSupervisedMOJOModel extends H2OSpecificMOJOLoader[H2OSupervisedMOJOModel]
