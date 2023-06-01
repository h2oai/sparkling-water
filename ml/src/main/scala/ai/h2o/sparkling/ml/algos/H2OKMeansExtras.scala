package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.{H2OColumnType, H2OFrame}
import hex.kmeans.KMeansModel.KMeansParameters

private[algos] trait H2OKMeansExtras extends H2OAlgorithm[KMeansParameters] {

  def getFoldCol(): String

  def setFoldCol(value: String): this.type

  override protected def prepareH2OTrainFrameForFitting(trainFrame: H2OFrame): Unit = {
    super.prepareH2OTrainFrameForFitting(trainFrame)
    val stringCols = trainFrame.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    if (stringCols.nonEmpty) {
      throw new IllegalArgumentException(
        s"Following columns are of type string: '${stringCols.mkString(", ")}', but" +
          " H2OKMeans does not accept string columns. However, you can use the 'columnsToCategorical' methods on H2OKMeans." +
          " These methods ensure that string columns are converted to representation H2O-3 understands.")
    }
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getFoldCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
