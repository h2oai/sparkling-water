package ai.h2o.sparkling.ml.params

trait H2OAutoMLParams
  extends H2OAlgorithmCommonParams
  with H2OAutoMLBuildControlParams
  with H2OAutoMLBuildModelsParams
  with H2OAutoMLInputParams
  with H2OAutoMLStoppingCriteriaParams
  with HasMonotoneConstraints
