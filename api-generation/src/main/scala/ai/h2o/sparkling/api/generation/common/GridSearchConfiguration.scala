package ai.h2o.sparkling.api.generation.common

import java.util

import hex.grid.HyperSpaceSearchCriteria
import hex.grid.HyperSpaceSearchCriteria.{CartesianSearchCriteria, RandomDiscreteValueSearchCriteria}
import hex.schemas.HyperSpaceSearchCriteriaV99
import hex.schemas.HyperSpaceSearchCriteriaV99.{CartesianSearchCriteriaV99, RandomDiscreteValueSearchCriteriaV99}

class GridSearchConfiguration extends SingleAlgorithmConfiguration {

  override def parametersConfiguration: Seq[ParameterSubstitutionContext] = {

    class DummySearchCriteria extends HyperSpaceSearchCriteriaV99[HyperSpaceSearchCriteria, DummySearchCriteria]

    val gridSearchParameters = Seq[(String, Class[_], Class[_], Seq[String])](
      (
        "H2OGridSearchRandomDiscreteCriteriaParams",
        classOf[RandomDiscreteValueSearchCriteriaV99],
        classOf[RandomDiscreteValueSearchCriteria],
        Seq("strategy")),
      (
        "H2OGridSearchCartesianCriteriaParams",
        classOf[CartesianSearchCriteriaV99],
        classOf[CartesianSearchCriteria],
        Seq("strategy")),
      ("H2OGridSearchCommonCriteriaParams", classOf[DummySearchCriteria], classOf[CartesianSearchCriteria], Seq.empty))

    for ((entityName, h2oSchemaClass: Class[_], h2oParameterClass: Class[_], extraIgnoredParameters) <- gridSearchParameters)
      yield ParameterSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName,
        h2oSchemaClass,
        h2oParameterClass,
        ignoredParameters = Seq("__meta") ++ extraIgnoredParameters,
        explicitFields = Seq.empty,
        deprecatedFields = Seq.empty,
        explicitDefaultValues = Map.empty,
        typeExceptions = Map.empty,
        defaultValueSource = DefaultValueSource.Getter,
        defaultValuesOfCommonParameters = Map(
          "algo" -> null,
          "hyperParameters" -> new util.HashMap[String, AnyRef](),
          "selectBestModelBy" -> "AUTO",
          "parallelism" -> 1),
        generateParamTag = false)
  }

  override def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = {
    Seq(
      AlgorithmSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.algos",
        "H2OGridSearch",
        null,
        "H2OAlgorithm",
        Seq("H2OGridSearchExtras"),
        false))
  }
}
