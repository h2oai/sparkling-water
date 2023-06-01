package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OAutoEncoderExtraParams
import org.apache.spark.sql.Row
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.linalg.DenseVector

import scala.collection.mutable

trait H2OAutoEncoderMOJOBase extends H2OFeatureMOJOModel with H2OAutoEncoderExtraParams {

  override protected def inputColumnNames: Array[String] = getInputCols()

  protected override def mojoUDF: UserDefinedFunction = {
    val schema = StructType(outputSchema)
    val uid = this.uid
    val mojoFileName = this.mojoFileName
    val withOrdinalCol = this.getWithOriginalCol()
    val withMSECol = this.getWithMSECol()
    val configInitializers = this.getEasyPredictModelWrapperConfigurationInitializers()
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(uid, mojoFileName, configInitializers)
      val pred = model.predictAutoEncoder(RowConverter.toH2ORowData(r))
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += new DenseVector(pred.reconstructed).compressed
      if (withOrdinalCol) resultBuilder += new DenseVector(pred.original).compressed
      if (withMSECol) resultBuilder += pred.mse
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }
}
