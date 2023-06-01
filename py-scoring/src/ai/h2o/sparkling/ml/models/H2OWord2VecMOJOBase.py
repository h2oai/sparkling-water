from ai.h2o.sparkling.ml.params.H2OMOJOModelParams import H2OFeatureMOJOModelParams
from ai.h2o.sparkling.ml.params.HasInputColOnMOJO import HasInputColOnMOJO
from ai.h2o.sparkling.ml.params.HasOutputColOnMOJO import HasOutputColOnMOJO


class H2OWord2VecMOJOBase(H2OFeatureMOJOModelParams, HasInputColOnMOJO, HasOutputColOnMOJO):
    pass
