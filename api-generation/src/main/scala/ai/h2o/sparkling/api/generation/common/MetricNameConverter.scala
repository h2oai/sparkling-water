package ai.h2o.sparkling.api.generation.common

object MetricNameConverter {
  val h2oToSWExceptions: Map[String, (String, String)] = Map(
    "cm" -> ("confusionMatrix", "ConfusionMatrix"),
    "AUC" -> ("auc", "AUC"),
    "RMSE" -> ("rmse", "RMSE"),
    "MSE" -> ("mse", "MSE"),
    "Gini" -> ("gini", "Gini"),
    "AIC" -> ("aic", "AIC"),
    "pr_auc" -> ("prauc", "PRAUC"),
    "mae" -> ("mae", "MAE"),
    "rmsle" -> ("rmsle", "RMSLE"),
    "multinomial_auc_table" -> ("multinomialAUCTable", "MultinomialAUCTable"),
    "multinomial_aucpr_table" -> ("multinomialPRAUCTable", "MultinomialPRAUCTable"),
    "numerr" -> ("numErr", "NumErr"),
    "numcnt" -> ("numCnt", "NumCnt"),
    "caterr" -> ("catErr", "CatErr"),
    "catcnt" -> ("catCnt", "CatCnt"))

  def convertFromH2OToSW(parameterName: String): (String, String) = {

    val parts = parameterName.split("_")
    val capitalizedParts = parts.head +: parts.tail.map(_.capitalize)
    val regularValue = capitalizedParts.mkString
    h2oToSWExceptions.getOrElse(parameterName, (regularValue, regularValue.capitalize))
  }
}
