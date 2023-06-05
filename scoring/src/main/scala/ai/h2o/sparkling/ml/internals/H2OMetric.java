package ai.h2o.sparkling.ml.internals;

public enum H2OMetric {
  AUTO(true),
  MeanResidualDeviance(false),
  MAE(false),
  RMSLE(false),
  R2(true),
  ResidualDeviance(false),
  ResidualDegreesOfFreedom(false),
  NullDeviance(false),
  NullDegreesOfFreedom(false),
  AIC(true),
  AUC(true),
  PRAUC(true),
  Gini(true),
  F1(true),
  F2(true),
  F0point5(true),
  Precision(true),
  Recall(true),
  MCC(true),
  Logloss(false),
  Error(false),
  MaxPerClassError(false),
  Accuracy(true),
  MSE(false),
  RMSE(false),
  Withinss(false),
  Betweenss(true),
  TotWithinss(false),
  Totss(false),
  MeanPerClassError(false),
  ScoringTime(false),
  Nobs(true),
  MeanNormalizedScore(false),
  MeanScore(false),
  Concordance(true),
  Concordant(true),
  Discordant(false),
  TiedY(true),
  NumErr(false),
  NumCnt(true),
  CatErr(false),
  CatCnt(true),
  Loglikelihood(true);

  public boolean higherTheBetter() {
    return higherTheBetter;
  }

  private boolean higherTheBetter;

  H2OMetric(boolean higherTheBetter) {
    this.higherTheBetter = higherTheBetter;
  }
}
