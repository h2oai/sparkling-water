package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OWord2VecExtraParams
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

trait H2OWord2VecMOJOBase extends H2OFeatureMOJOModel with H2OWord2VecExtraParams {

  override protected def inputColumnNames: Array[String] = Array(getInputCol())

  override def transform(dataset: Dataset[_]): DataFrame = {
    validate(dataset.schema)
    super.transform(dataset)
  }

  protected override def mojoUDF: UserDefinedFunction = {
    val schema = StructType(outputSchema)
    val uid = this.uid
    val mojoFileName = this.mojoFileName
    val configInitializers = this.getEasyPredictModelWrapperConfigurationInitializers()
    val inputCol = getInputCol()
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(uid, mojoFileName, configInitializers)
      val colIdx = model.m.getColIdx(inputCol)
      val pred = if (r.isNullAt(colIdx)) {
        null
      } else {
        model.predictWord2Vec(r.getSeq[String](colIdx).toArray)
      }
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

}
