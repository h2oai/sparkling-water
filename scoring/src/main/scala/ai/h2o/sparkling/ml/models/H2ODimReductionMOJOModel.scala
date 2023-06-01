package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2ODimReductionExtraParams
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType

trait H2ODimReductionMOJOModel extends H2OFeatureMOJOModel with H2ODimReductionExtraParams {

  override protected def inputColumnNames: Array[String] = getInputCols()

  protected def reconstructedEnabled: Boolean = false

  protected override def mojoUDF: UserDefinedFunction = {
    val schema = StructType(outputSchema)
    val uid = this.uid
    val mojoFileName = this.mojoFileName
    val configInitializers = getEasyPredictModelWrapperConfigurationInitializers()
    val reconstructedEnabled = this.reconstructedEnabled
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(uid, mojoFileName, configInitializers)
      val pred = model.predictDimReduction(RowConverter.toH2ORowData(r))
      val output = new DenseVector(pred.dimensions).compressed
      val rowData = if (reconstructedEnabled) {
        val reconstructed = new DenseVector(pred.reconstructed).compressed
        Array[Any](output, reconstructed)
      } else {
        Array[Any](output)
      }
      new GenericRowWithSchema(rowData, schema)
    }
    udf(function, schema)
  }
}
