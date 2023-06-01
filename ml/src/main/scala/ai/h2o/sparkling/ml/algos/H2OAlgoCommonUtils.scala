package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.backend.utils.H2OFrameLifecycle
import ai.h2o.sparkling.ml.models.H2OBinaryModel
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.col

trait H2OAlgoCommonUtils extends EstimatorCommonUtils with H2OFrameLifecycle {

  protected var binaryModel: Option[H2OBinaryModel] = None

  def getBinaryModel(): H2OBinaryModel = {
    if (binaryModel.isEmpty) {
      throw new IllegalArgumentException(
        "Algorithm needs to be fit first with the `keepBinaryModels` parameter " +
          "set to true in order to access binary model.")
    }
    binaryModel.get
  }

  private[sparkling] def getExcludedCols(): Seq[String] = Seq.empty

  /** The list of additional columns that needs to be send to H2O-3 backend for model training. */
  private[sparkling] def getAdditionalCols(): Seq[String] = Seq.empty

  /** The list of additional columns that needs to be send to H2O-3 backend for model validation. */
  private[sparkling] def getAdditionalValidationCols(): Seq[String] = Seq.empty

  private[sparkling] def getInputCols(): Array[String]

  private[sparkling] def getColumnsToCategorical(): Array[String]

  /** List of columns to convert to string before modelling. */
  private[sparkling] def getColumnsToString(): Array[String] = Array.empty

  private[sparkling] def getSplitRatio(): Double

  private[sparkling] def setInputCols(value: Array[String]): this.type

  private[sparkling] def getValidationDataFrame(): DataFrame

  private[sparkling] def prepareDatasetForFitting(dataset: Dataset[_]): (H2OFrame, Option[H2OFrame]) = {
    val excludedCols = getExcludedCols()

    if (getInputCols().isEmpty) {
      val inputs = dataset.columns.filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
      setInputCols(inputs)
    } else {
      val missingColumns = getInputCols()
        .filterNot(col => dataset.columns.contains(col))

      if (missingColumns.nonEmpty) {
        throw new IllegalArgumentException(
          "The following feature columns are not available on" +
            s" the training dataset: '${missingColumns.mkString(", ")}'")
      }
    }

    val featureColumns = getInputCols().map(sanitize).map(col)

    val excludedColumns = excludedCols.map(sanitize).map(col)
    val additionalColumns = getAdditionalCols().map(sanitize).map(col)
    val columns = (featureColumns ++ excludedColumns ++ additionalColumns).distinct
    val h2oContext = H2OContext.ensure(
      "H2OContext needs to be created in order to train the model. Please create one as H2OContext.getOrCreate().")
    val trainFrame = h2oContext.asH2OFrame(dataset.select(columns: _*).toDF(), getInputCols())

    trainFrame.convertColumnsToStrings(getColumnsToString())

    // Our MOJO wrapper needs the full column name before the array/vector expansion in order to do predictions
    trainFrame.convertColumnsToCategorical(getColumnsToCategorical())

    val validationDataFrame = getValidationDataFrame()
    val (resultTrainFrame, resultTestFrame) = if (validationDataFrame != null) {
      val additionalValidationColumns = getAdditionalValidationCols().map(sanitize).map(col)
      val validationColumns = (columns ++ additionalValidationColumns).distinct
      val validationFrame = h2oContext.asH2OFrame(validationDataFrame.select(validationColumns: _*))
      (trainFrame, Some(validationFrame))
    } else if (getSplitRatio() < 1.0) {
      val frames = trainFrame.split(getSplitRatio())
      if (frames.length > 1) {
        (frames(0), Some(frames(1)))
      } else {
        (frames(0), None)
      }
    } else {
      (trainFrame, None)
    }

    registerH2OFrameForDeletion(resultTrainFrame)
    registerH2OFrameForDeletion(resultTestFrame)

    (resultTrainFrame, resultTestFrame)
  }

  def sanitize(colName: String) = '`' + colName + '`'
}
