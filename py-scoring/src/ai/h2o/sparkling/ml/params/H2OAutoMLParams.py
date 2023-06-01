from ai.h2o.sparkling.ml.params.H2OAutoMLBuildControlParams import H2OAutoMLBuildControlParams
from ai.h2o.sparkling.ml.params.H2OAutoMLBuildModelsParams import H2OAutoMLBuildModelsParams
from ai.h2o.sparkling.ml.params.H2OAutoMLInputParams import H2OAutoMLInputParams
from ai.h2o.sparkling.ml.params.H2OAutoMLStoppingCriteriaParams import H2OAutoMLStoppingCriteriaParams
from ai.h2o.sparkling.ml.params.H2OCommonParams import H2OCommonParams
from ai.h2o.sparkling.ml.params.HasMonotoneConstraints import HasMonotoneConstraints
from pyspark.ml.param import *


class H2OAutoMLParams(
    H2OCommonParams,
    H2OAutoMLBuildControlParams,
    H2OAutoMLBuildModelsParams,
    H2OAutoMLInputParams,
    H2OAutoMLStoppingCriteriaParams,
    HasMonotoneConstraints
):
    pass
